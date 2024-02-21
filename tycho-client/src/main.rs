use std::time::Duration;
use tycho_client::{
    deltas::DeltasClient,
    feed::{
        component_tracker::ComponentFilter, synchronizer::ProtocolStateSynchronizer,
        BlockSynchronizer,
    },
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

    let extractor_id = ExtractorIdentity { chain: Chain::Ethereum, name: "uniswap_v3".to_string() };
    let synchronizer = ProtocolStateSynchronizer::new(
        extractor_id.clone(),
        true,
        ComponentFilter::Ids(vec!["0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".to_string()]),
        1, // TODO can it be 0?
        HttpRPCClient::new(&tycho_rpc_url).unwrap(),
        ws_client.clone(),
    );

    let block_sync = BlockSynchronizer::new(Duration::from_secs(360), Duration::from_secs(1))
        .register_synchronizer(extractor_id, synchronizer);

    let (jh, mut rx) = block_sync
        .run()
        .await
        .expect("block sync start error");

    while let Some(msg) = rx.recv().await {
        dbg!(msg);
    }

    dbg!("RX closed");
    jh.await.unwrap();
}
