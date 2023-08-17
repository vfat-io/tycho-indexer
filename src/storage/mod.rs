pub mod orm;
pub mod postgres;
pub mod schema;

use std::{collections::HashMap, error::Error};

use async_trait::async_trait;

use crate::models::{Chain, ExtractorInstance, ProtocolSystem};

pub enum BlockIdentifier<'a> {
    Number(i64),
    Hash(&'a [u8]),
}

#[async_trait]
pub trait ChainGateway {
    type Block;
    type Transaction;

    async fn add_block(&self, new: Self::Block) -> Result<(), Box<dyn Error>>;
    async fn get_block(&self, id: BlockIdentifier<'_>) -> Result<Self::Block, Box<dyn Error>>;
    async fn add_tx(&self, new: Self::Transaction) -> Result<(), Box<dyn Error>>;
    async fn get_tx(&self, hash: &[u8]) -> Result<Self::Transaction, Box<dyn Error>>;
}

#[async_trait]
pub trait ExtractorInstanceGateway {
    async fn get_state(
        &self,
        name: &str,
        chain: Chain,
    ) -> Result<Option<ExtractorInstance>, Box<dyn Error>>;

    async fn save_state(&self, state: ExtractorInstance) -> Result<(), Box<dyn Error>>;
}

#[async_trait]
pub trait ProtocolGateway {
    type Token;
    type ProtocolComponent;

    async fn get_component(
        &self,
        chain: Chain,
        system: ProtocolSystem,
        id: &str,
    ) -> Result<Self::ProtocolComponent, Box<dyn Error>>;

    async fn add_component(&self, new: Self::ProtocolComponent) -> Result<(), Box<dyn Error>>;

    async fn get_system(&self, system: ProtocolSystem) -> Result<ProtocolSystem, Box<dyn Error>>;

    async fn add_system(&self, system: ProtocolSystem) -> Result<(), Box<dyn Error>>;

    async fn get_token(&self, address: &[u8]) -> Result<Self::Token, Box<dyn Error>>;

    async fn add_tokens(&self, token: &[Self::Token]) -> Result<(), Box<dyn Error>>;
}

#[async_trait]
pub trait ContractStateGateway {
    type ContractState;
    type Slot;
    type Value;

    async fn get_contract(
        &mut self,
        address: &[u8],
        at_tx: Option<&[u8]>,
    ) -> Result<Self::ContractState, Box<dyn Error>>;

    async fn add_contract(&mut self, new: Self::ContractState) -> Result<(), Box<dyn Error>>;

    async fn delete_contract(
        &self,
        address: &[u8],
        at_tx: Option<&[u8]>,
    ) -> Result<(), Box<dyn Error>>;

    async fn get_contract_slots(
        &self,
        address: &[u8],
        at_tx: Option<&[u8]>,
    ) -> Result<HashMap<Self::Slot, Self::Value>, Box<dyn Error>>;

    async fn upsert_slots(
        &self,
        address: &[u8],
        modify_tx: &[u8],
        slots: HashMap<Self::Slot, Self::Value>,
    ) -> Result<(), Box<dyn Error>>;

    async fn delete_slots(
        &self,
        address: &[u8],
        delete_tx: &[u8],
        slots: Vec<Self::Slot>,
    ) -> Result<(), Box<dyn Error>>;

    async fn get_slots_delta(
        &self,
        address: &[u8],
        start_tx: Option<&[u8]>,
        end_tx: Option<&[u8]>,
    ) -> Result<HashMap<Self::Slot, Self::Value>, Box<dyn Error>>;

    async fn revert_contract_state(
        &self,
        address: &[u8],
        to_tx: &[u8],
    ) -> Result<(), Box<dyn Error>>;
}
