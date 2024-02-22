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

/// Run a simple example of a block synchronizer.
///
/// You need to port-forward tycho before running this:
///
/// ```bash
/// kubectl port-forward deploy/tycho-indexer 8888:4242
/// ```
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

    let v3_id = ExtractorIdentity { chain: Chain::Ethereum, name: "uniswap_v3".to_string() };
    let v3_sync = ProtocolStateSynchronizer::new(
        v3_id.clone(),
        true,
        ComponentFilter::Ids(vec!["0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".to_string()]),
        1, // TODO can it be 0?
        HttpRPCClient::new(&tycho_rpc_url).unwrap(),
        ws_client.clone(),
    );
    let v2_id = ExtractorIdentity { chain: Chain::Ethereum, name: "uniswap_v2".to_string() };
    let v2_sync = ProtocolStateSynchronizer::new(
        v2_id.clone(),
        true,
        ComponentFilter::Ids(vec!["0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc".to_string()]),
        1,
        HttpRPCClient::new(&tycho_rpc_url).unwrap(),
        ws_client.clone(),
    );

    let block_sync = BlockSynchronizer::new(Duration::from_secs(600), Duration::from_secs(1))
        .register_synchronizer(v3_id, v3_sync)
        .register_synchronizer(v2_id, v2_sync);

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
