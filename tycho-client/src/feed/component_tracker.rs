use crate::{rpc::RPCClient, HttpRPCClient};
use std::collections::{HashMap, HashSet};
use tycho_types::{
    dto::{
        Chain, ExtractorIdentity, ProtocolComponent, ProtocolComponentRequestParameters,
        ProtocolComponentsRequestBody, ProtocolId,
    },
    Bytes,
};

/// Helper struct to store which components are being tracked atm.
pub struct ComponentTracker {
    chain: Chain,
    protocol_system: String,
    min_tvl_threshold: f64,
    // We will need to request a snapshot for components/Contracts that we did not emit as
    // snapshot for yet but are relevant now, e.g. because min tvl threshold exceeded.
    pub components: HashMap<String, ProtocolComponent>,
    /// derived from tracked components, we need this if subscribed to a vm extractor cause updates
    /// are emitted on a contract level instead of on a component level.
    pub contracts: HashSet<Bytes>,
    /// Client to retrieve necessary protocol components from the rpc.
    rpc_client: HttpRPCClient,
}

impl ComponentTracker {
    pub fn new() -> Self {
        todo!();
    }
    /// Retrieve all components that belong to the system we are extracing and have sufficient tvl.
    pub async fn initialise_components(&mut self) {
        let filters = ProtocolComponentRequestParameters::tvl_filtered(self.min_tvl_threshold);
        let request = ProtocolComponentsRequestBody::system_filtered(&self.protocol_system);
        self.components = self
            .rpc_client
            .get_protocol_components(self.chain, &filters, &request)
            .await
            .expect("could not init protocol components")
            .protocol_components
            .into_iter()
            .map(|pc| (pc.id.clone(), pc))
            .collect::<HashMap<_, _>>();
        self.update_contracts();
    }

    pub fn update_contracts(&mut self) {
        self.contracts.extend(
            self.components
                .values()
                .flat_map(|comp| comp.contract_ids.iter().cloned()),
        );
    }

    /// Add a new component to be tracked
    pub async fn start_tracking(&mut self, new_components: &[&String]) {
        let filters = ProtocolComponentRequestParameters::default();
        let request = ProtocolComponentsRequestBody::id_filtered(
            new_components
                .iter()
                .map(|pc_id| pc_id.to_string())
                .collect(),
        );

        self.components.extend(
            self.rpc_client
                .get_protocol_components(self.chain, &filters, &request)
                .await
                .expect("could not get new protocol components")
                .protocol_components
                .into_iter()
                .map(|pc| (pc.id.clone(), pc)),
        );
        self.update_contracts();
    }

    /// Stop tracking components
    pub async fn stop_tracking<'a, I: IntoIterator<Item = &'a String>>(
        &mut self,
        to_remove: I,
    ) -> HashMap<String, ProtocolComponent> {
        to_remove
            .into_iter()
            .filter_map(|k| {
                let comp = self.components.remove(k);
                if let Some(component) = &comp {
                    for contract in component.contract_ids.iter() {
                        self.contracts.remove(contract);
                    }
                }
                comp.map(|c| (k.clone(), c))
            })
            .collect()
    }

    pub fn get_contracts_by_component<'a, I: IntoIterator<Item = &'a str>>(
        &self,
        ids: I,
    ) -> HashSet<Bytes> {
        ids.into_iter()
            .flat_map(|cid| {
                let comp = self
                    .components
                    .get(cid)
                    .expect("requested component that is not present");
                comp.contract_ids.iter().cloned()
            })
            .collect()
    }
    pub fn get_tracked_component_ids(&self) -> Vec<ProtocolId> {
        todo!()
    }
}
