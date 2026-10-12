use hydro_deploy::Deployment;
use hydro_examples::thumbnail::{Service, thumbnail_service};
use hydro_lang::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let address: std::net::SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:8080".into())
        .parse()
        .expect("address must be IP:port");
    assert!(address.port() > 0, "port must be nonzero");
    assert!(
        args.next().is_none(),
        "usage: thumbnail_local [bind_address]"
    );
    let mut deployment = Deployment::new();
    let mut flow = FlowBuilder::new();
    let service = flow.process::<Service>();
    thumbnail_service(&service, address);
    let _nodes = flow
        .with_process(&service, deployment.Localhost())
        .deploy(&mut deployment);
    deployment.deploy().await.unwrap();
    deployment.start_ctrl_c().await.unwrap();
}
