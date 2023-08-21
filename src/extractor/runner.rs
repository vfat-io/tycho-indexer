use std::{collections::HashMap, error::Error, sync::Arc};

use tokio::{
    sync::mpsc::{self, error::SendError, Receiver, Sender},
    task::JoinHandle,
};
use tokio_stream::StreamExt;

use super::Extractor;
use crate::{
    models::NormalisedMessage,
    substreams::stream::{BlockResponse, SubstreamsStream},
};

pub enum ControlMessage<M> {
    Stop,
    Subscribe(Sender<Arc<M>>),
}

pub struct ExtractorHandle<M> {
    handle: JoinHandle<()>,
    control_tx: Sender<ControlMessage<M>>,
}

impl<M> ExtractorHandle<M>
where
    M: NormalisedMessage + Sync + Send + 'static,
{
    async fn subscribe(&self) -> Result<Receiver<Arc<M>>, SendError<ControlMessage<M>>> {
        let (tx, rx) = mpsc::channel(1);
        self.control_tx.send(ControlMessage::Subscribe(tx)).await?;
        Ok(rx)
    }

    async fn stop(self) -> Result<(), Box<dyn Error>> {
        self.control_tx.send(ControlMessage::Stop).await?;
        self.handle.await?;
        Ok(())
    }
}

pub struct ExtractorRunner<G, M> {
    extractor: Arc<dyn Extractor<G, M>>,
    substreams: SubstreamsStream,
    subscriptions: HashMap<u64, Sender<Arc<M>>>,
    control_rx: Receiver<ControlMessage<M>>,
}

impl<G, M> ExtractorRunner<G, M>
where
    M: NormalisedMessage + Sync + Send + 'static,
    G: Sync + Send + 'static,
{
    pub fn run(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(ctrl) = self.control_rx.recv() =>  {
                        match ctrl {
                            ControlMessage::Stop => {
                                break;
                            },
                            ControlMessage::Subscribe(sender) => {
                                let counter = self.subscriptions.len() as u64;
                                self.subscriptions.insert(counter, sender);
                            },
                        }
                    }
                    val = self.substreams.next() => {
                        match val {
                            None => {
                                println!("Stream consumed");
                                break;
                            }
                            Some(Ok(BlockResponse::New(data))) => {
                                if let Ok(msg) = self.extractor.handle_tick_scoped_data(data).await {
                                    if let Some(msg) = msg {
                                        Self::propagate_msg(&self.subscriptions, msg).await
                                    }
                                } else {
                                    println!("Error while processing tick!");
                                    break;
                                }
                            }
                            Some(Ok(BlockResponse::Undo(undo_signal))) => {
                                if let Ok(msg) = self.extractor.handle_revert(undo_signal).await {
                                    if let Some(msg) = msg {
                                        Self::propagate_msg(&self.subscriptions, msg).await
                                    }
                                } else {
                                    println!("Error while processing revert!");
                                    break;
                                }
                            }
                            Some(Err(err)) => {
                                println!();
                                println!("Stream terminated with error");
                                println!("{:?}", err);
                                break;
                            }
                        };
                    }
                }
            }
        })
    }

    async fn propagate_msg(subscribers: &HashMap<u64, Sender<Arc<M>>>, message: M) {
        let arced_message = Arc::new(message);
        for s in subscribers.values() {
            s.send(arced_message.clone()).await.unwrap();
        }
    }
}

pub fn start_extractor<G, M>(
    extractor: Arc<dyn Extractor<G, M>>,
    substreams: SubstreamsStream,
) -> ExtractorHandle<M>
where
    M: NormalisedMessage + Sync + Send + 'static,
    G: Sync + Send + 'static,
{
    let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
    let runner = ExtractorRunner {
        extractor,
        substreams,
        subscriptions: HashMap::new(),
        control_rx: ctrl_rx,
    };

    ExtractorHandle {
        handle: runner.run(),
        control_tx: ctrl_tx,
    }
}
