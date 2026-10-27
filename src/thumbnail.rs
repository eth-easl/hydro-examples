//! One stateless HTTP service per platform-managed instance.
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use std::io::Cursor;

use crate::http::{Bridge, Config};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;

pub struct Service;
pub struct Archive;

enum Delivery {
    Client(crate::http::Message<Response, Response>),
    Archive(Response),
}

pub const MAX_DIMENSION: u32 = 2048;
pub const MAX_BODY: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Request {
    pub width: u32,
    pub height: u32,
    pub image: Vec<u8>,
}

pub type Response = Result<Vec<u8>, String>;

fn validate_dimensions(width: u32, height: u32) -> Result<(), String> {
    if !(1..=MAX_DIMENSION).contains(&width) || !(1..=MAX_DIMENSION).contains(&height) {
        return Err(format!("dimensions must be between 1 and {MAX_DIMENSION}"));
    }
    Ok(())
}

/// Fit within the requested box, preserving aspect ratio and avoiding upscaling.
/// Accept PNG/JPEG and always produce PNG. No filesystem or external storage.
pub fn render(request: Request) -> Response {
    validate_dimensions(request.width, request.height)?;
    if request.image.len() > MAX_BODY {
        return Err("image exceeds 8 MiB".into());
    }
    let mut reader = image::ImageReader::new(Cursor::new(request.image))
        .with_guessed_format()
        .map_err(|e| e.to_string())?;
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(8192);
    limits.max_image_height = Some(8192);
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    let decoded = reader.decode().map_err(|e| e.to_string())?;
    let thumbnail = decoded.thumbnail(
        request.width.min(decoded.width()),
        request.height.min(decoded.height()),
    );
    let mut output = Cursor::new(Vec::new());
    thumbnail
        .write_to(&mut output, image::ImageFormat::Png)
        .map_err(|e| e.to_string())?;
    Ok(output.into_inner())
}

#[derive(Deserialize)]
struct Size {
    width: u32,
    height: u32,
}

async fn handle_thumbnail(
    State(bridge): State<Bridge<Request, Response>>,
    Query(size): Query<Size>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(error) = validate_dimensions(size.width, size.height) {
        return (StatusCode::BAD_REQUEST, error).into_response();
    }
    match bridge
        .call(Request {
            width: size.width,
            height: size.height,
            image: body.to_vec(),
        })
        .await
    {
        Ok(Ok(bytes)) => (
            [("content-type", "image/png"), ("cache-control", "no-store")],
            bytes,
        )
            .into_response(),
        Ok(Err(error)) => (StatusCode::UNPROCESSABLE_ENTITY, error).into_response(),
        Err(error) => error.into_response(),
    }
}

pub fn thumbnail_service<'a>(
    service: &Process<'a, Service>,
    archive: &Process<'a, Archive>,
    address: std::net::SocketAddr,
) {
    let (requests, responses) = crate::http::serve(
        service,
        address,
        Config::default(),
        q!(|bridge| {
            Router::new()
                .route("/health", axum::routing::get(|| async { "ok" }))
                .route("/thumbnail", axum::routing::post(self::handle_thumbnail))
                .layer(DefaultBodyLimit::max(self::MAX_BODY))
                .with_state(bridge)
        }),
    );
    let (client_items, archive_items) = requests
        .flat_map_ordered(q!(|request| {
            let reply = crate::http::Message::map(request, self::render);
            let archive_copy = reply.payload.clone();

            [
                self::Delivery::Client(reply),
                self::Delivery::Archive(archive_copy),
            ]
        }))
        .partition(q!(|item| { matches!(item, self::Delivery::Client(_)) }));

    // Reply to the original HTTP client.
    responses.complete(
        client_items
            .filter_map(q!(|item| match item {
                self::Delivery::Client(reply) => Some(reply),
                _ => None,
            }))
            .weaken_ordering::<NoOrder>(),
    );

    // Send to the archive Hydro process.
    archive_items
        .filter_map(q!(|item| match item {
            self::Delivery::Archive(result) => Some(result),
            _ => None,
        }))
        .send(archive, TCP.fail_stop().bincode())
        .for_each(q!(|_result| {
            // Intentionally ignore the received result.
        }));
}

async fn handle_archive(
    State(bridge): State<Bridge<Vec<u8>, ()>>,
    body: Bytes,
) -> axum::response::Response {
    match bridge.call(body.to_vec()).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

pub fn archive_service<'a>(
    archive: &Process<'a, Archive>,
    address: std::net::SocketAddr,
) {
    let (requests, responses) = crate::http::serve(
        archive,
        address,
        Config::default(),
        q!(|bridge| {
            Router::new()
                .route("/health", axum::routing::get(|| async { "ok" }))
                .route("/archive", axum::routing::post(self::handle_archive))
                .layer(DefaultBodyLimit::max(self::MAX_BODY))
                .with_state(bridge)
        }),
    );

    // Receive each HTTP request and discard its payload.
    responses.complete(
        requests
            .map(q!(|request| {
                crate::http::Message::map(request, |_bytes| ())
            }))
            .weaken_ordering::<NoOrder>(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(format: image::ImageFormat) -> Vec<u8> {
        let image = image::RgbImage::from_pixel(80, 40, image::Rgb([23, 80, 160]));
        let mut bytes = Cursor::new(Vec::new());
        image.write_to(&mut bytes, format).unwrap();
        bytes.into_inner()
    }

    #[test]
    fn formats_aspect_ratio_and_no_upscaling() {
        for format in [image::ImageFormat::Png, image::ImageFormat::Jpeg] {
            for (width, height, expected) in
                [(20, 20, (20, 10)), (200, 200, (80, 40)), (80, 5, (10, 5))]
            {
                let response = render(Request {
                    width,
                    height,
                    image: fixture(format),
                });
                let output = image::load_from_memory_with_format(
                    &response.unwrap(),
                    image::ImageFormat::Png,
                )
                .unwrap();
                assert_eq!((output.width(), output.height()), expected);
            }
        }
    }

    #[test]
    fn invalid_requests_are_errors() {
        for (width, height, bytes) in [
            (0, 1, fixture(image::ImageFormat::Png)),
            (1, 2049, vec![]),
            (1, 1, b"not an image".to_vec()),
            (1, 1, vec![0; MAX_BODY + 1]),
        ] {
            assert!(
                render(Request {
                    width,
                    height,
                    image: bytes
                })
                .is_err()
            );
        }
    }
}
