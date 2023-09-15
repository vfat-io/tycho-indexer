// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub parent_hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub number: u64,
    #[prost(uint64, tag = "4")]
    pub ts: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub from: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub to: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub index: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContractSlot {
    #[prost(bytes = "vec", tag = "2")]
    pub slot: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub value: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContractChange {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    /// empty bytes indicates no change
    #[prost(bytes = "vec", tag = "2")]
    pub balance: ::prost::alloc::vec::Vec<u8>,
    /// empty bytes indicates no change
    #[prost(bytes = "vec", tag = "3")]
    pub code: ::prost::alloc::vec::Vec<u8>,
    /// empty sequence indicates no change
    #[prost(message, repeated, tag = "4")]
    pub slots: ::prost::alloc::vec::Vec<ContractSlot>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionChanges {
    #[prost(message, optional, tag = "1")]
    pub tx: ::core::option::Option<Transaction>,
    #[prost(message, repeated, tag = "2")]
    pub contract_changes: ::prost::alloc::vec::Vec<ContractChange>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockContractChanges {
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<Block>,
    #[prost(message, repeated, tag = "2")]
    pub changes: ::prost::alloc::vec::Vec<TransactionChanges>,
}
// @@protoc_insertion_point(module)
