use chrono::NaiveDateTime;

use crate::raw;

pub enum WebSocketMessage<T: From<raw::BlockAccountChanges>> {
    BlockAccountChanges(T),
    Response(raw::Response),
}

impl<T> From<raw::WebSocketMessage> for WebSocketMessage<T>
where
    T: From<raw::BlockAccountChanges>,
{
    fn from(value: raw::WebSocketMessage) -> Self {
        todo!()
    }
}

pub struct ContractId<A: From<Vec<u8>>> {
    pub address: A,
    pub chain: raw::Chain,
}

impl<T> From<raw::ContractId> for ContractId<T>
where
    T: From<Vec<u8>>,
{
    fn from(value: raw::ContractId) -> Self {
        todo!()
    }
}

pub struct Version<B: From<raw::Block>> {
    timestamp: NaiveDateTime,
    block: Option<B>,
}

impl<B> From<raw::VersionParam> for Version<B>
where
    B: From<raw::Block>,
{
    fn from(value: raw::VersionParam) -> Self {
        todo!()
    }
}

pub struct StateRequestBody<A: From<Vec<u8>>, B: From<raw::Block>> {
    pub contract_ids: Option<Vec<ContractId<A>>>,
    pub version: Version<B>,
}

impl<A, B> From<raw::StateRequestBody> for StateRequestBody<A, B>
where
    A: From<Vec<u8>>,
    B: From<raw::Block>,
{
    fn from(value: raw::StateRequestBody) -> Self {
        todo!()
    }
}

pub struct StateRequestResponse<A: From<raw::ResponseAccount>> {
    pub accounts: Vec<A>,
}

impl<A> StateRequestResponse<A>
where
    A: From<raw::ResponseAccount>,
{
    pub fn new(accounts: Vec<A>) -> Self {
        Self { accounts }
    }
}

impl<A> From<raw::StateRequestBody> for StateRequestResponse<A>
where
    A: From<raw::ResponseAccount>,
{
    fn from(value: raw::StateRequestBody) -> Self {
        todo!()
    }
}
