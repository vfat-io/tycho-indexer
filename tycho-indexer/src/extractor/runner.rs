use anyhow::{format_err, Context, Result};
use async_trait::async_trait;
use prost::Message;
use std::{collections::HashMap, env, sync::Arc};
use tokio::{
    sync::{
        mpsc::{self, error::SendError, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::{debug, error, warn};

use super::Extractor;
use crate::{
    extractor::ExtractionError,
    models::{ExtractorIdentity, NormalisedMessage},
    pb::sf::substreams::v1::Package,
    substreams::{
        stream::{BlockResponse, SubstreamsStream},
        SubstreamsEndpoint,
    },
};

pub enum ControlMessage<M> {
    Stop,
    Subscribe(Sender<Arc<M>>),
}

/// A trait for a message sender that can be used to subscribe to messages
///
/// Extracted out of the [ExtractorHandle] to allow for easier testing
#[async_trait]
pub trait MessageSender<M: NormalisedMessage>: Send + Sync {
    async fn subscribe(&self) -> Result<Receiver<Arc<M>>, SendError<ControlMessage<M>>>;
}

#[derive(Clone)]
pub struct ExtractorHandle<M> {
    id: ExtractorIdentity,
    control_tx: Sender<ControlMessage<M>>,
}

impl<M> ExtractorHandle<M>
where
    M: NormalisedMessage,
{
    fn new(id: ExtractorIdentity, control_tx: Sender<ControlMessage<M>>) -> Self {
        Self { id, control_tx }
    }

    pub fn get_id(&self) -> ExtractorIdentity {
        self.id.clone()
    }

    pub async fn stop(&self) -> Result<(), ExtractionError> {
        // TODO: send a oneshot along here and wait for it
        self.control_tx
            .send(ControlMessage::Stop)
            .await
            .map_err(|err| ExtractionError::Unknown(err.to_string()))
    }
}

#[async_trait]
impl<M> MessageSender<M> for ExtractorHandle<M>
where
    M: NormalisedMessage,
{
    async fn subscribe(&self) -> Result<Receiver<Arc<M>>, SendError<ControlMessage<M>>> {
        let (tx, rx) = mpsc::channel(1);
        self.control_tx
            .send(ControlMessage::Subscribe(tx))
            .await?;

        Ok(rx)
    }
}

// Define the SubscriptionsMap type alias
type SubscriptionsMap<M> = HashMap<u64, Sender<Arc<M>>>;

pub struct ExtractorRunner<G, M> {
    extractor: Arc<dyn Extractor<G, M>>,
    substreams: SubstreamsStream,
    subscriptions: Arc<Mutex<SubscriptionsMap<M>>>,
    control_rx: Receiver<ControlMessage<M>>,
}

impl<G, M> ExtractorRunner<G, M>
where
    M: NormalisedMessage,
    G: Sync + Send + 'static,
{
    pub fn run(mut self) -> JoinHandle<Result<(), ExtractionError>> {
        tokio::spawn(async move {
            let id = self.extractor.get_id();
            loop {
                tokio::select! {
                    Some(ctrl) = self.control_rx.recv() =>  {
                        match ctrl {
                            ControlMessage::Stop => {
                                warn!("{}: Stop signal received; exiting!", &id);
                                return Ok(())
                            },
                            ControlMessage::Subscribe(sender) => {
                                self.subscribe(sender).await;
                            },
                        }
                    }
                    val = self.substreams.next() => {
                        match val {
                            None => {
                                return Err(ExtractionError::SubstreamsError(format!("{id}: stream ended")));
                            },
                            Some(Ok(BlockResponse::New(data))) => {
                                let block = data.clock.as_ref().map(|v| v.number).unwrap_or(0);
                                match self.extractor.handle_tick_scoped_data(data).await {
                                    Ok(Some(msg)) => {
                                        debug!("{}:{}: Propagating new data message.", &block, &id);
                                        Self::propagate_msg(&self.subscriptions, msg).await
                                    }
                                    Ok(None) => {
                                        debug!("{}:{}: No message to propagate.", &block ,&id);
                                    }
                                    Err(err) => {
                                        error!("{block}:{id}: Error while processing tick: {err}!");
                                        return Err(err);
                                    }
                                }
                            }
                            Some(Ok(BlockResponse::Undo(undo_signal))) => {
                                let block = undo_signal.last_valid_block.as_ref().map(|v| v.number).unwrap_or(0);
                                warn!("{id}: Revert to {block} requested!");
                                match self.extractor.handle_revert(undo_signal).await {
                                    Ok(Some(msg)) => {
                                        debug!("{id}: Propagating undo message.");
                                        Self::propagate_msg(&self.subscriptions, msg).await
                                    }
                                    Ok(None) => {
                                        debug!("{id}: No undo message to propagate.");
                                    }
                                    Err(err) => {
                                        error!("{id}: Error while processing revert to {block}!");
                                        return Err(err);
                                    }
                                }
                            }
                            Some(Err(err)) => {
                                error!("{}: Stream terminated with error {:#?}", id, err);
                                return Err(ExtractionError::SubstreamsError(err.to_string()));
                            }
                        };
                    }
                }
            }
        })
    }

    async fn subscribe(&mut self, sender: Sender<Arc<M>>) {
        let counter = self.subscriptions.lock().await.len() as u64;
        self.subscriptions
            .lock()
            .await
            .insert(counter, sender);
    }

    async fn propagate_msg(subscribers: &Arc<Mutex<SubscriptionsMap<M>>>, message: M) {
        debug!("Propagating message: {:?}", message);
        let arced_message = Arc::new(message);

        let mut to_remove = Vec::new();

        // Lock the subscribers HashMap for exclusive access
        let mut subscribers = subscribers.lock().await;

        for (counter, sender) in subscribers.iter_mut() {
            match sender.send(arced_message.clone()).await {
                Ok(_) => {
                    // Message sent successfully
                    debug!("Message sent to subscriber {}", counter);
                }
                Err(_) => {
                    // Receiver has been dropped, mark for removal
                    to_remove.push(*counter);
                    error!("Subscriber {} has been dropped", counter);
                }
            }
        }

        // Remove inactive subscribers
        for counter in to_remove {
            subscribers.remove(&counter);
        }
    }
}

pub struct ExtractorRunnerBuilder<G, M> {
    spkg_file: String,
    endpoint_url: String,
    module_name: String,
    start_block: i64,
    end_block: i64,
    token: String,
    extractor: Arc<dyn Extractor<G, M>>,
}

impl<G, M> ExtractorRunnerBuilder<G, M>
where
    M: NormalisedMessage,
    G: Sync + Send + 'static,
{
    pub fn new(spkg: &str, extractor: Arc<dyn Extractor<G, M>>) -> Self {
        Self {
            spkg_file: spkg.to_owned(),
            endpoint_url: "https://mainnet.eth.streamingfast.io:443".to_owned(),
            module_name: "map_changes".to_owned(),
            start_block: 0,
            end_block: 0,
            token: env::var("SUBSTREAMS_API_TOKEN").unwrap_or("".to_string()),
            extractor,
        }
    }

    #[allow(dead_code)]
    pub fn endpoint_url(mut self, val: &str) -> Self {
        self.endpoint_url = val.to_owned();
        self
    }

    #[allow(dead_code)]
    pub fn module_name(mut self, val: &str) -> Self {
        self.module_name = val.to_owned();
        self
    }

    pub fn start_block(mut self, val: i64) -> Self {
        self.start_block = val;
        self
    }

    pub fn end_block(mut self, val: i64) -> Self {
        self.end_block = val;
        self
    }

    #[allow(dead_code)]
    pub fn token(mut self, val: &str) -> Self {
        self.token = val.to_owned();
        self
    }

    pub async fn run(
        self,
    ) -> Result<(JoinHandle<Result<(), ExtractionError>>, ExtractorHandle<M>), ExtractionError>
    {
        let content = std::fs::read(&self.spkg_file)
            .context(format_err!("read package from file '{}'", self.spkg_file))
            .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?;
        let spkg = Package::decode(content.as_ref())
            .context("decode command")
            .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?;
        let endpoint = Arc::new(
            SubstreamsEndpoint::new(&self.endpoint_url, Some(self.token))
                .await
                .map_err(|err| ExtractionError::SubstreamsError(err.to_string()))?,
        );
        let cursor = self.extractor.get_cursor().await;
        let stream = SubstreamsStream::new(
            endpoint,
            Some(cursor),
            spkg.modules.clone(),
            self.module_name,
            self.start_block,
            self.end_block as u64,
        );

        let id = self.extractor.get_id();
        let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
        let runner = ExtractorRunner {
            extractor: self.extractor,
            substreams: stream,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            control_rx: ctrl_rx,
        };

        let handle = runner.run();
        Ok((handle, ExtractorHandle::new(id, ctrl_tx)))
    }
}
