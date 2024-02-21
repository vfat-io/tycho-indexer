use tycho_client::{
    deltas::DeltasClient,
    feed::synchronizer::{ProtocolStateSynchronizer, StateSynchronizer},
    HttpRPCClient, WsDeltasClient,
};
use tycho_types::dto::{Chain, ExtractorIdentity};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let tycho_url = "localhost:8888";
    let tycho_ws_url = format!("ws://{tycho_url}");
    let tycho_rpc_url = format!("http://{tycho_url}");
    let ws_client = WsDeltasClient::new(&tycho_ws_url).unwrap();
    ws_client
        .connect()
        .await
        .expect("ws client connection error");
    let synchronizer = ProtocolStateSynchronizer::new(
        ExtractorIdentity { chain: Chain::Ethereum, name: "uniswap_v3".to_string() },
        true,
        10.0,
        1, // TODO can it be 0?
        HttpRPCClient::new(&tycho_rpc_url).unwrap(),
        ws_client.clone(),
    );

    let (jh, mut rx) = synchronizer.start().await.unwrap();

    while let Some(header) = rx.recv().await {
        dbg!(&header);
        let msg = synchronizer
            .get_pending(header.hash.clone())
            .await
            .expect("get pending error");

        dbg!(msg);
    }

    dbg!("RX closed");
    jh.await.unwrap().unwrap();
}
