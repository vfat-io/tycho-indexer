// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContractStorageChange {
    #[prost(bytes="vec", tag="1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="2")]
    pub slot: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="3")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag="4")]
    pub log_ordinal: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockStorageChanges {
    #[prost(uint64, tag="1")]
    pub number: u64,
    #[prost(bytes="vec", tag="2")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag="3")]
    pub changes: ::prost::alloc::vec::Vec<ContractStorageChange>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Changes {
    #[prost(message, repeated, tag="1")]
    pub changes: ::prost::alloc::vec::Vec<BlockStorageChanges>,
}
// @@protoc_insertion_point(module)
