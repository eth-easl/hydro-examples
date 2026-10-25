# Hydro examples

This demo runs a stateless thumbnail application inside one Hydro `Process`,
with an Axum HTTP server connected through `sidecar_bidi`:

```text
HTTP request → sidecar → Hydro map(render) → sidecar → HTTP response
```

Each instance is independent. A hosting platform can route requests between
instances and scale their number; there is no Hydro cluster or shared application
state. The example includes a local launcher, not platform deployment packaging.

## Run

From this directory:

```sh
cargo run --example thumbnail_local -- 127.0.0.1:8080
```

The address is optional and defaults to `127.0.0.1:8080`. Wait for `HTTP listening`;
Hydro compiles and launches the service process. Ctrl-C stops the deployment.

```sh
curl --fail-with-body --data-binary @photo.jpg \
  -H 'Content-Type: application/octet-stream' \
  'http://127.0.0.1:8080/thumbnail?width=320&height=240' \
  --output thumbnail.png
```

`POST /thumbnail` accepts PNG/JPEG bytes and returns a PNG that fits the requested
box, preserves aspect ratio, and never upscales. EXIF orientation is not applied.
`GET /health` returns `ok`.

Basic limits keep the demo manageable:

- Width and height: 1–2048 (`400` for invalid dimensions).
- Request body: up to 8 MiB (`413` above that).
- Decoding: up to 8192 pixels per axis and a 128 MiB decoder allocation budget (`422` for invalid, unsupported, or
  oversized images).
- Each sidecar channel holds up to 64 queued items. A full request queue returns
  `503`; this is a queue limit, not a total in-flight request limit.
- Waiting for a reply times out after 30 seconds (`504`). Closed processing
  channels return `503`.

Image processing runs synchronously on Hydro's single runtime thread, so HTTP
handling and timeout polling pause during rendering. Timeouts are cooperative;
client cancellation does not stop work already submitted. This is a demonstration
of stateless application execution, not a production HTTP runtime or benchmark.

## Reuse the HTTP component

`src/http.rs` is independent of the thumbnail application. It provides
`Bridge<Input, Output>` for Axum handlers and `serve` to attach a router to a
Hydro process. Applications supply routes, validation, and response formatting.

1. Call `http::serve(&process, address, config, q!(|bridge| { /* router */ }))`.
   The router factory executes in the deployed process and uses the bridge as
   Axum state.
2. In a handler, call `bridge.call(input).await` and format the output. Bridge
   errors implement `IntoResponse` (`503` or `504`).
3. Transform each request with `message.map(process)` and complete the response
   stream. The thumbnail example uses:

```rust
responses.complete(
requests
.map(q!(|message| message.map(self::render)))
.weaken_ordering::<NoOrder>(),
);
```

Each `Message<Input, Output>` carries its own one-shot reply channel. Mapping it
produces `Message<Output, Output>` while preserving that channel, so results go
back to the right caller without request IDs or a shared response registry.
Messages move through a local dataflow; they cannot be cloned or serialized.
Payloads only need `Send + 'static`.

`Config` sets channel capacity and reply timeout. For an application-managed
server, `connect` returns the bridge and raw sidecar channels; `start` also binds
and starts an Axum server. See `src/thumbnail.rs` for the complete integration.

## Verify

```sh
cargo test --lib
# With the service running:
python3 scripts/smoke_thumbnail.py
# Or specify its address:
python3 scripts/smoke_thumbnail.py http://127.0.0.1:9090
```

The tests cover image processing, reply routing, and basic bridge errors. The
smoke test exercises the generated Hydro process through HTTP, including
concurrent requests.

## Running with new serverless infrastructure

In order to generate Hydro processes for a (Knative) serverless infrastructure, use KnativeDeploy and then call
`generate_deploy_files()` on the deployment. This will build a Docker image for each process and push it to your local
registry. Furthermore, a corresponding deployment YAML for the Knative serverless infrastructure will be generated for
each process in `target/hydro_trybuild/<project-name>/deploy` (<project-name> is "hydro-examples" in this project). Copy
the built image from your docker registry to the Knative control plane and apply the generated YAML. For a local Knative
setup using Kind that runs Knative inside docker containers, use port forwarding to enable ingress traffic from your
host machine to the Knative cluster:

```bash
kubectl -n kourier-system port-forward svc/kourier 8080:80
```

Note that in order for this routing to work, you must bind Axum to `0.0.0.0:<port>`, where `<port>` matches the exposed
port of the corresponding hydro service. Next, find the Knative URL for your service (possibly requires replacing the service name in the following command):

```bash
kubectl get ksvc hy-service-loc1v1 -o jsonpath='{.status.url}'
```

Using this URL, check the health of your service:

```bash
curl -i \
  -H "Host: hy-service-loc1v1.default.127.0.0.1.sslip.io" \
  http://127.0.0.1:8080/health
```

Test the thumbnail service with a request as follows:

```bash
curl --fail-with-body \
  --data-binary @photo.jpg \
  -H 'Content-Type: application/octet-stream' \
  -H 'Host: hy-service-loc1v1.default.127.0.0.1.sslip.io' \
  'http://127.0.0.1:8080/thumbnail?width=320&height=240' \
  --output thumbnail.png
```