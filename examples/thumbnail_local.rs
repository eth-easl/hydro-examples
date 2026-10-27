use hydro_deploy::Deployment;
use hydro_lang::deploy::{HydroManifest, KnativeDeploy};
use hydro_examples::thumbnail::{thumbnail_service, Archive, Service, archive_service};
use hydro_lang::prelude::*;
use hydro_lang::runtime_support::dfir_rs::serde_json;

fn main() {
    // SAFETY: This runs before Tokio is initialized or
    // any application threads are spawned.
    unsafe {
        std::env::set_var(
            "CARGO_MANIFEST_DIR",
            env!("CARGO_MANIFEST_DIR"),
        );
    }

    println!(
        "CARGO_MANIFEST_DIR: {:?}",
        std::env::var("CARGO_MANIFEST_DIR")
    );

    run();
}


#[tokio::main(flavor = "current_thread")]
async fn run() {
    println!(
        "Current working directory: {:?}",
        std::env::current_dir().unwrap()
    );

    println!(
        "Executable: {:?}",
        std::env::current_exe().unwrap()
    );

    println!(
        "Runtime CARGO_MANIFEST_DIR: {:?}",
        std::env::var("CARGO_MANIFEST_DIR")
    );

    println!("Working directory: {:?}", std::env::current_dir());
    println!("CARGO_MANIFEST_DIR: {:?}", std::env::var("CARGO_MANIFEST_DIR"));
    println!("CARGO: {:?}", std::env::var("CARGO"));

    let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest = std::path::Path::new(&dir).join("Cargo.toml");

    println!("Manifest path: {:?}", manifest);
    println!("Manifest exists: {}", manifest.is_file());



    let mut args = std::env::args().skip(1);
    let address: std::net::SocketAddr = args
        .next()
        .unwrap_or_else(|| "0.0.0.0:8080".into())
        .parse()
        .expect("address must be IP:port");
    assert!(address.port() > 0, "port must be nonzero");
    assert!(
        args.next().is_none(),
        "usage: thumbnail_local [bind_address]"
    );
    let mut deployment = KnativeDeploy::new();
    // let mut deployment = Deployment::new();
    let mut flow = FlowBuilder::new();
    let service = flow.process::<Service>();
    let archive = flow.process::<Archive>();
    // let external = flow.external::<()>();
    thumbnail_service(&service, &archive, address);
    archive_service(&archive, address);
    // let _nodes = flow
    //     .with_process(&service, deployment.add_knative_process())
    //     .with_external(&external, deployment.add_external("client".into()))
    //     .deploy(&mut deployment);

    let _nodes = flow
        .with_process(&service, deployment.add_knative_process())
        .with_process(&archive, deployment.add_knative_process())
        .deploy(&mut deployment);

    // Stop after code generation if requested.
    // if std::env::var_os("HYDRO_GENERATE_ONLY").is_some() {
    //     println!("Generated Hydro process source files.");
    //     return;
    // }

    _nodes.get_process(&service).expose_port(8080);
    _nodes.get_process(&archive).expose_port(8080);

    deployment.generate_deploy_files(&_nodes).await.unwrap();

    let manifest: HydroManifest = deployment.export(&_nodes);

    std::fs::write("hydro-manifest.json", serde_json::to_string_pretty(&manifest).unwrap()).unwrap();

    // deployment.deploy().await.unwrap();
    // deployment.start_ctrl_c().await.unwrap();
}
