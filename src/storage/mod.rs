//! # Storage Traits
//!
//! This module encapsulates the traits and structs meant for handling
//! operations such as retrieval, addition, and deletion pertaining to protocol
//! state.
//!
//! ## Versioning
//!
//! The core of Tycho keeps track of all states by timestamp versioning. This
//! strategy allows us to maintain and retrieve state across different
//! protocols, including those that rely on various clock mechanisms (e.g.,
//! blockchains).
//!
//! In addition to timestamps, blockchain state is further versioned using
//! transactions. Since a block carries a single timestamp only, there can be
//! instances where one block's timestamp might overlap with multiple states. In
//! these cases, an explicit input is required from the user. For example, a
//! user may want to track all states within a block or just the most recent
//! one. As every onchain state modification associates with a transaction
//! having an index, the original sequence of state modifications stays
//! preserved.
//!
//! ## Implementations
//!
//! To set up a storage system, you need to implement all the traits defined
//! below. Additionally, the entities you aim to store must also implement the
//! respective `Storable*` trait.
//!
//! Note that you will have different entities based on the specific blockchain
//! under consideration. For instance, entities for EVM and Starknet will vary!
//!
//! The gateways are not confined to a certain chain scope but are universally
//! applicable over a range of entity types. So, a gateway designed for EVM
//! entities can handle multiple EVM-based chains, like mainnet & arbitrum.
//!
//! However, if the entities for the chains differ, you may need to resort to
//! separate gateway instances. Alternatively, you can create an enum that
//! houses all different entity types and then implement the respective traits
//! for these enums. Following this approach paves the way for initializing a
//! cross-chain compatible gateway (For instance, refer
//! [enum_dispatch](https://docs.rs/enum_dispatch/latest/enum_dispatch/) crate).
pub mod postgres;

use std::{collections::HashMap, fmt::Display, sync::Arc};

use crate::services::deserialization_helpers::{chain_from_str, hex_to_bytes};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde::Deserialize;
use thiserror::Error;

use crate::models::{Chain, ExtractionState, ProtocolComponent, ProtocolSystem};

// We use variable length bytes type for literals of the contract store for
// flexibility and because postgres does not support a fixed size byte array
// column type. On blockchains these are usually fixed, e.g. 32 bytes on
// ethereuE but might have different lengths (e.g. addresses in Ethereum has 20
// bytes length vs Starknet uses 32 byte addresses).

/// Address hash literal type to uniquely identify contracts/accounts on a
/// blockchain.
type Address = Vec<u8>;
/// Block hash literal type to uniquely identify a block in the chain and
/// likely across chains.
type BlockHash = Vec<u8>;

type AddressRef<'a> = &'a [u8];
type BlockHashRef<'a> = &'a [u8];
type TxHashRef<'a> = &'a [u8];

/// Identifies a block in storage.
#[derive(Debug)]
pub enum BlockIdentifier {
    /// Identifies the block by its position on a specified chain.
    ///
    /// This form of identification has potential risks as it may become
    /// ambiguous in certain situations.For example, if the block has not been
    /// finalised, there exists a possibility of forks occurring. As a result,
    /// the same number could refer to different blocks on different forks.
    Number((Chain, i64)),

    /// Identifies a block by its hash.
    ///
    /// The hash should be unique across multiple chains. Preferred method if
    /// the block is very recent.
    Hash(BlockHash),
}

impl Display for BlockIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Lays out the necessary interface needed to store and retrieve blocks from
/// storage.
///
/// Generics:
/// * `S`: This represents the storage-specific data type used when converting from storage to the
///   block.
/// * `N`: This represents the storage-specific data type used when converting from the block to
///   storage.
/// * `I`: Represents the type of the database identifier, which is used as an argument in the
///   conversion function. This facilitates the passage of database-specific foreign keys to the
///   `to_storage` method, thereby providing a flexible way for different databases to interact with
///   the block.
///
/// It defines methods for converting from a storage-specific type to a block,
/// converting from a block to a storage-specific type, and getting the block's
/// chain.
pub trait StorableBlock<S, N, I>: Send + Sync + 'static {
    /// Constructs a block from a storage-specific value `val` and a `Chain`.
    ///
    /// # Arguments
    ///
    /// * `val` - The storage-specific representation of the block
    /// * `chain` - The chain associated with the block
    ///
    /// # Returns
    ///
    /// A block constructed from `val` and `chain`
    fn from_storage(val: S, chain: Chain) -> Self;

    /// Converts the block to a storage-specific representation.
    ///
    /// # Arguments
    ///
    /// * `chain_id` - The id of the chain that the block belongs to
    ///
    /// # Returns
    ///
    /// The storage-specific representation of the block
    fn to_storage(&self, chain_id: I) -> N;

    /// Returns the `Chain` object associated with the block.
    ///
    /// # Returns
    ///
    /// The `Chain` that the block is associated with
    fn chain(&self) -> Chain;
}

/// Lays out the necessary interface needed to store and retrieve transactions
/// from storage.
///
/// Generics:
/// * `S`: This represents the storage-specific data type used when converting from storage to the
///   transaction.
/// * `N`: This represents the storage-specific data type used when converting from the transaction
///   to storage.
/// * `I`: Represents the type of the database identifier, which is used as an argument in the
///   conversion function. This facilitates the passage of database-specific foreign keys to the
///   `to_storage` method, thereby providing a flexible way for different databases to interact with
///   the transaction.
pub trait StorableTransaction<S, N, I>: Send + Sync + 'static {
    /// Converts a transaction from storage representation (`S`) to transaction
    /// form. This function uses the original block hash, where the
    /// transaction resides, for this conversion.
    fn from_storage(val: S, block_hash: BlockHashRef) -> Self;

    /// Converts a transaction object to its storable representation (`N`),
    /// while also associating it with a specific block through a database ID
    /// (`I`).
    fn to_storage(&self, block_id: I) -> N;

    /// Returns the block hash associated with a transaction. This is
    /// necessary to ensure that transactions can be traced back to the blocks
    /// from which they originated.
    fn block_hash(&self) -> BlockHashRef;

    /// Returns the hash associated with this transaction, which
    /// uniquely identifies it.
    fn hash(&self) -> TxHashRef;
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Could not find {0} with id `{1}`!")]
    NotFound(String, String),
    #[error("The entity {0} with id {1} was already present!")]
    DuplicateEntry(String, String),
    #[error("Could not find related {0} for {1} with id `{2}`!")]
    NoRelatedEntity(String, String, String),
    #[error("DecodeError: {0}")]
    DecodeError(String),
    #[error("Unexpected storage error: {0}")]
    Unexpected(String),
    #[error("Currently unsupported operation: {0}")]
    Unsupported(String),
}

/// Storage methods for chain specific objects.
///
/// This trait abstracts the specific implementation details of a blockchain's
/// entities, allowing the user to add and retrieve blocks and transactions in a
/// generic way.
///
/// For traceability protocol components and contracts changes are linked to
/// blocks of their respective chain if applicable. This means while indexing we
/// need to keep a lightweight and cross chain compatible representation of
/// blocks and transactions in storage.
///
/// It's defined generically over two associated types:
///
/// * `Block`: represents a block in the blockchain.
/// * `Transaction`: represents a transaction within a block.
#[async_trait]
pub trait ChainGateway {
    type DB;
    type Block;
    type Transaction;

    /// Upserts a new block to the blockchain's storage.
    ///
    /// Ignores any existing tx, if the new entry has different attributes
    /// no error is raised and the old entry is kept.
    ///
    /// # Parameters
    /// - `new`: An instance of `Self::Block`, representing the new block to be stored.
    ///
    /// # Returns
    /// - Empty ok result indicates success. Failure might occur if the block is already present.
    async fn upsert_block(&self, new: &Self::Block, db: &mut Self::DB) -> Result<(), StorageError>;
    /// Retrieves a block from storage.
    ///
    /// # Parameters
    /// - `id`: Block's unique identifier of type `BlockIdentifier`.
    ///
    /// # Returns
    /// - An Ok result containing the block. Might fail if the block does not exist yet.
    async fn get_block(
        &self,
        id: &BlockIdentifier,
        db: &mut Self::DB,
    ) -> Result<Self::Block, StorageError>;
    /// Upserts a transaction to storage.
    ///
    /// Ignores any existing tx, if the new entry has different attributes
    /// no error is raised and the old entry is kept.
    ///
    /// # Parameters
    /// - `new`: An instance of `Self::Transaction`, representing the new transaction to be stored.
    ///
    /// # Returns
    /// - Empty ok result indicates success. Failure might occur if the
    /// corresponding block does not exists yet, or if the transaction already
    /// exists.
    async fn upsert_tx(
        &self,
        new: &Self::Transaction,
        db: &mut Self::DB,
    ) -> Result<(), StorageError>;

    /// Tries to retrieve a transaction from the blockchain's storage using its
    /// hash.
    ///
    /// # Parameters
    /// - `hash`: The byte slice representing the hash of the transaction to be retrieved.
    ///
    /// # Returns
    /// - An Ok result containing the transaction. Might fail if the transaction does not exist yet.
    async fn get_tx(
        &self,
        hash: &[u8],
        db: &mut Self::DB,
    ) -> Result<Self::Transaction, StorageError>;
}

/// Store and retrieve state of Extractors.
///
/// Sometimes extractors may wish to persist their state across restart. E.g.
/// substreams based extractors need to store the cursor, so they can continue
/// processing where they left off.
///
/// Extractors are uniquely identified by a name and the respective chain which
/// they are indexing.
#[async_trait]
pub trait ExtractionStateGateway {
    type DB;

    /// Retrieves the state of an extractor instance from a storage.
    ///
    /// # Parameters
    /// - `name` A unique name for the extractor instance.
    /// - `chain` The chain this extractor is indexing.
    ///
    /// # Returns
    /// Ok if the corrsponding state was retrieved successfully, Err in
    /// case the state was not found.
    async fn get_state(
        &self,
        name: &str,
        chain: Chain,
        conn: &mut Self::DB,
    ) -> Result<ExtractionState, StorageError>;

    /// Saves the state of an extractor instance to a storage.
    ///
    /// Creates an entry if not present yet, or updates an already existing
    /// entry.
    ///
    /// # Parameters
    /// - `state` The state of the extractor that needs to be saved.
    ///
    /// # Returns
    /// Ok, if state was stored successfully, Err if the state is not valid.
    async fn save_state(
        &self,
        state: &ExtractionState,
        conn: &mut Self::DB,
    ) -> Result<(), StorageError>;
}

/// Point in time as either block or timestamp. If a block is chosen it
/// timestamp attribute is used.
#[derive(Debug)]
pub enum BlockOrTimestamp {
    Block(BlockIdentifier),
    Timestamp(NaiveDateTime),
}

/// References certain states within a single block.
///
/// **Note:** Not all methods that take a version will support all version kinds,
/// the versions here are included for completeness and to document the
/// retrieval behaviour that is possible with the storage layout. Please refer
/// to the individual implementation for information about which version kinds
/// it supports.
#[derive(Debug, Default)]
pub enum VersionKind {
    /// Represents the final state within a specific block. Essentially, it
    /// retrieves the state subsequent to the execution of the last transaction
    /// executed in that block.
    #[default]
    Last,
    /// Represents the initial state of a specific block. In other words,
    /// it is the state before any transaction has been executed within that block.
    First,
    /// Represents a specific transactions indexed position within a block.
    /// It includes the state after executing the transaction at that index.
    Index(i64),
}
#[derive(Debug, Deserialize, PartialEq)]
pub struct ContractId {
    #[serde(deserialize_with = "hex_to_bytes")]
    address: Vec<u8>,
    #[serde(deserialize_with = "chain_from_str")]
    chain: Chain,
}

/// Uniquely identifies a contract on a specific chain.
impl ContractId {
    pub fn new(chain: Chain, address: Address) -> Self {
        Self { address, chain }
    }
}

impl Display for ContractId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: 0x{}", self.chain, hex::encode(&self.address))
    }
}

/// A version desribes the state of the DB at a exact point in time.
/// See the module level docs for more information on how versioning works.
pub struct Version(pub BlockOrTimestamp, pub VersionKind);

impl Version {
    pub fn from_block_number(chain: Chain, number: i64) -> Self {
        Self(BlockOrTimestamp::Block(BlockIdentifier::Number((chain, number))), VersionKind::Last)
    }
    pub fn from_ts(ts: NaiveDateTime) -> Self {
        Self(BlockOrTimestamp::Timestamp(ts), VersionKind::Last)
    }
}
/// Lays out the necessary interface needed to store and retrieve tokens from
/// storage.
///
/// Generics:
/// * `S`: This represents the storage-specific data type used when converting from storage to the
///   token.
/// * `N`: This represents the storage-specific data type used when converting from the token to
///   storage.
/// * `I`: Represents the type of the database identifier, which is used as an argument in the
///   conversion function. This facilitates the passage of database-specific foreign keys to the
///   `to_storage` method, thereby providing a flexible way for different databases to interact with
///   the token.
pub trait StorableToken<S, N, I>: Send + Sync + 'static {
    fn from_storage(val: S, contract: ContractId) -> Self;

    fn to_storage(&self, contract_id: I) -> N;

    fn contract_id(&self) -> ContractId;
}

/// Store and retrieve protocol related structs.
///
/// This trait defines how to retrieve protocol components, state as well as
/// tokens from storage.
#[async_trait]
pub trait ProtocolGateway {
    type DB;
    type Token;
    // TODO: at this later type ProtocolState;

    /// Retrieve ProtocolComponent from the db
    ///
    /// # Parameters
    /// - `chain` The chain of the component
    /// - `system` Allows to optionally filter by system.
    /// - `id` Allows to optionally filter by id.
    ///
    /// # Returns
    /// Ok, if found else Err
    async fn get_components(
        &self,
        chain: Chain,
        system: Option<ProtocolSystem>,
        ids: Option<&[&str]>,
    ) -> Result<Vec<ProtocolComponent>, StorageError>;

    /// Stores new found ProtocolComponents.
    ///
    /// Components are assumed to be immutable. Any state belonging to a
    /// component that is dynamic, should be made available on ProtocolState,
    /// not on the Component.
    ///
    /// # Parameters
    /// - `new`  The new protocol components.
    ///
    /// # Returns
    /// Ok if stored successfully, may error if:
    /// - related entities are not in store yet.
    /// - component with same is id already present.
    async fn upsert_components(&self, new: &[&ProtocolComponent]) -> Result<(), StorageError>;

    /// Retrieve protocol component states
    ///
    /// This resource is versioned, the version can be specified by either block
    /// or timestamp, for off-chain components, a block version will error.
    ///
    /// As the state is retained on a transaction basis on blockchain systems, a
    /// single version may relate to more than one state. In these cases a
    /// versioned result is returned, if requesting `Version:All` with the
    /// latest entry being the state at the end of the block and the first entry
    /// represents the first change to the state within the block.
    ///
    /// # Parameters
    /// - `chain` The chain of the component
    /// - `system` The protocol system this component belongs to
    /// - `id` The external id of the component e.g. address, or the pair
    /// - `at` The version at which the state is valid at.
    /*
    // TODO: implement once needed
    type ProtocolState;
    async fn get_states(
        &self,
        chain: Chain,
        at: Option<Version>,
        system: Option<ProtocolSystem>,
        id: Option<&[&str]>,
    ) -> Result<VersionedResult<Self::ProtocolState>, StorageError>;
     */

    /// Retrieves a tokens from storage
    ///
    /// # Parameters
    /// - `chain` The chain this token is implemented on.
    /// - `address` The address for the token within the chain.
    ///
    /// # Returns
    /// Ok if the results could be retrieved from the storage, else errors.
    async fn get_tokens(
        &self,
        chain: Chain,
        address: Option<&[AddressRef]>,
    ) -> Result<Vec<Self::Token>, StorageError>;

    /// Saves multiple tokens to storage.
    ///
    /// Inserts token into storage. Tokens and their properties are assumed to
    /// be immutable.
    ///
    /// # Parameters
    /// - `chain` The chain of the token.
    /// - `token` The tokens to insert.
    ///
    /// # Return
    /// Ok if all tokens could be inserted, Err if at least one token failed to
    /// insert.
    async fn add_tokens(&self, chain: Chain, token: &[&Self::Token]) -> Result<(), StorageError>;
}

/// Key literal type of the contract store.
type StoreKey = Vec<u8>;
/// Value literal type of the contract store.
type StoreVal = Vec<u8>;
/// A binary key value store for an account.
type ContractStore = HashMap<StoreKey, Option<StoreVal>>;
/// Multiple key values stores grouped by account address.
type AccountToContractStore = HashMap<Address, ContractStore>;
/// A set of changes to stores, grouped by the modifying transaction hash.
type SlotChangeSet<'a> = &'a [(TxHashRef<'a>, AccountToContractStore)];

/// Lays out the necessary interface needed to store and retrieve contracts from
/// and their associated state from storage.
///
/// Generics:
/// * `S`: This represents the storage-specific data type used when converting from storage to the
///   contract.
/// * `N`: This represents the storage-specific data type used when converting from the contract to
///   storage.
/// * `I`: Represents the type of the database identifier, which is used as an argument in the
///   conversion function. This facilitates the passage of database-specific foreign keys to the
///   `to_storage` method, thereby providing a flexible way for different databases to interact with
///   the contract.
pub trait StorableContract<S, N, I>: Send + Sync + 'static {
    /// Creates a transaction from storage.
    ///
    /// # Parameters:
    /// * `val`: State as retrieved from storage.
    /// * `chain`: The blockchain where this contract resides.
    /// * `balance_modify_tx`: Transaction hash reference that modified the balance.
    /// * `code_modify_tx`: Transaction hash reference that modified the code.
    /// * `creation_tx`: Transaction hash reference that created the contract.
    fn from_storage(
        val: S,
        chain: Chain,
        balance_modify_tx: TxHashRef,
        code_modify_tx: TxHashRef,
        creation_tx: Option<TxHashRef>,
    ) -> Self;

    /// Transforms the state of the contract into it's storable form.
    ///
    /// # Parameters:
    /// * `chain_id`: Identifier for the chain
    /// * `creation_ts`: Timestamp when the contract was created
    /// * `tx_id`: Identifier of the transaction
    fn to_storage(&self, chain_id: I, creation_ts: NaiveDateTime, tx_id: Option<I>) -> N;

    /// Get the chain where this contract resides.
    fn chain(&self) -> Chain;

    /// Get the transaction hash that created this contract if it exists.
    ///
    /// # Note
    /// We allow the creation transaction to be optional as sometimes we need to
    /// insert old contracts and finding the original transaction that created
    /// it during indexing is hard. Thus this is optional but should be always
    /// set when the contract creation is actually observed. Contracts with this
    /// field unset will not be deleted on during a revert.
    fn creation_tx(&self) -> Option<TxHashRef>;

    /// Get a reference to the address of this contract.
    fn address(&self) -> AddressRef;

    /// Get the transaction that last modified the balance of this contract.
    fn balance_modify_tx(&self) -> TxHashRef;

    /// Get the transaction that last modified the code of this contract.
    fn code_modify_tx(&self) -> TxHashRef;

    /// Get a copy of this contract's store in it's storable form.
    fn store(&self) -> ContractStore;

    /// Replace the current store of this contract with a new one.
    ///
    /// # Parameters:
    /// * `store`: The new contract store as retrieved from storage.
    ///
    /// # Errors:
    /// This method will return an error if the replacement is not successful.
    /// E.g. if the passed store value fails to convert into this structs types.
    fn set_store(&mut self, store: &ContractStore) -> Result<(), StorageError>;
}

/// Manage contracts and their state in storage.
///
/// Specifies how to retrieve, add and update contracts in storage.
#[async_trait]
pub trait ContractStateGateway {
    type DB;
    type ContractState;

    /// Get a contracts state from storage
    ///
    /// This method retrieves balance and code, but not storage.
    ///
    /// # Parameters
    /// - `id` The identifier for the contract.
    /// - `version` Version at which to retrieve state for. None retrieves the latest state.
    async fn get_contract(
        &self,
        id: &ContractId,
        version: &Option<&Version>,
        db: &mut Self::DB,
    ) -> Result<Self::ContractState, StorageError>;

    /// Get multiple contracts' states from storage.
    ///
    /// This method retrieves balance and code, and optionally storage, of
    /// multiple contracts in a chain. It can optionally filter by given
    /// addresses and retrieve state for specific versions.
    ///
    /// # Parameters:
    /// - `chain`: The blockchain where the contracts reside.
    /// - `addresses`: Filter for specific addresses. If set to `None`, it retrieves all indexed
    ///   contracts in the chain.
    /// - `version`: Version at which to retrieve state for. If set to `None`, it retrieves the
    ///   latest state.
    /// - `include_slots`: Flag to determine whether to include slot changes. If set to `true`, it
    ///   includes storage slot.
    /// - `db`: Database session reference.
    ///
    /// # Returns:
    /// A `Result` with a list of contract states if the operation is
    /// successful, or a `StorageError` if the operation fails.
    async fn get_contracts(
        &self,
        chain: Chain,
        addresses: Option<&[AddressRef]>,
        version: Option<&Version>,
        include_slots: bool,
        db: &mut Self::DB,
    ) -> Result<Vec<Self::ContractState>, StorageError>;

    /// Save contract metadata.
    ///
    /// Inserts immutable contract metadata, as well as code and balance.
    ///
    /// # Parameters
    /// - `new` the new contract state to be saved.
    async fn add_contract(
        &self,
        new: &Self::ContractState,
        db: &mut Self::DB,
    ) -> Result<(), StorageError>;

    /// Mark a contract as deleted
    ///
    /// Issues a soft delete of the contract.
    ///
    /// # Parameters
    /// - `id` The identifier for the contract.
    /// - `at_tx` The transaction hash which deleted the contract. This transaction is assumed to be
    ///   in storage already. None retrieves the latest state.
    /// - `db` The database handle or connection.
    ///
    /// # Returns
    /// Ok if the deletion was successful, might Err if:
    ///  - Contract is not present in storage.
    ///  - Deletion transaction is not present in storage.
    ///  - Contract was already deleted.
    async fn delete_contract(
        &self,
        id: &ContractId,
        at_tx: TxHashRef<'_>,
        db: &mut Self::DB,
    ) -> Result<(), StorageError>;

    /// Retrieve contract storage.
    ///
    /// Retrieve the storage slots of contracts at a given time/version.
    ///
    /// Will return the slots state after the given block/timestamp. Later we
    /// might change to use VersionResult, but for now we keep it simple. Using
    /// anything else then Version::Last is currently not supported.
    ///
    /// # Parameters
    /// - `chain` The chain for which to retrieve slots for.
    /// - `addresses` Optionally allows filtering by contract address.
    /// - `at` The version at which to retrieve slots. None retrieves the latest
    /// - `db` The database handle or connection. state.
    async fn get_contract_slots(
        &self,
        chain: Chain,
        addresses: Option<&[AddressRef]>,
        at: Option<&Version>,
        db: &mut Self::DB,
    ) -> Result<AccountToContractStore, StorageError>;

    /// Upserts slots for a given contract.
    ///
    /// Upserts slots from multiple accounts. To correctly track changes, it is
    /// necessary that each slot modification has a corresponding transaction
    /// assigned.
    ///
    /// # Parameters
    /// - `slots` A slice containing only the changed slots. Including slots that were changed to 0.
    ///   Includes slot changes grouped per account and indexed by the transaction hash that
    ///   modified them.
    ///
    /// # Returns
    /// An empty `Ok(())` if the operation succeeded. Will raise an error if any
    /// of the related entities can not be found: e.g. one of the referenced
    /// transactions or accounts is not or not yet persisted.
    async fn upsert_slots(
        &self,
        slots: SlotChangeSet<'_>,
        db: &mut Self::DB,
    ) -> Result<(), StorageError>;
    /// Retrieve a slot delta between two versions
    ///
    /// Given start version V1 and end version V2, this method will return the
    /// changes necessary to move from V1 to V2. So if V1 < V2, it will contain
    /// the value of all slots that changed between the two versions with the
    /// values corresponding to V2. If V2 < V1 then it will contain all the
    /// slots that changed between the two versions with the values
    /// corresponding to V1.
    ///
    /// # Parameters
    /// - `id` The identifier for the contract.
    /// - `chain` The chain for which to generate the delta changes. This is required because
    ///   version might be a timestamp.
    /// - `start_version` The deltas start version, given a block uses VersionKind::Last behaviour.
    ///   If None the latest version is assumed.
    /// - `end_version` The deltas end version, given a block uses VersionKind::Last behaviour.
    ///
    /// # Note
    ///
    /// A choice to utilize `BlockOrTimestamp` has been made intentionally in
    /// this scenario as passing a `Version` by user isn't quite logical.
    /// Support for deltas is limited to the states at the start or end of
    /// blocks because blockchain reorganization at the transaction level is not
    /// common.
    ///
    /// The decision to use either the beginning or end state of a block is
    /// automatically determined by the underlying logic. For example, if we are
    /// tracing back, `VersionKind::First` retrieval mode will be used.
    /// Conversely, if we're progressing forward, we would apply the
    /// `VersionKind::Last` semantics.
    ///
    /// # Returns
    /// A map containing the necessary changes to update a state from
    /// start_version to end_version.
    /// Errors if:
    ///     - The versions can't be located in storage.
    ///     - There was an error with the database
    async fn get_slots_delta(
        &self,
        id: Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
        db: &mut Self::DB,
    ) -> Result<AccountToContractStore, StorageError>;

    /// Reverts the contract in storage to a previous version.
    ///
    /// This modification will delete version in storage. The state will be
    /// reset to the passed version.
    ///
    /// Note:
    /// This method is scoped to a chain via the id parameter. All changes on
    /// that chain will be reverted to the target version.
    ///
    /// # Parameters
    /// - `id` The identifier for the contract.
    /// - `to` The version to revert to. Given a block uses VersionKind::Last behaviour.
    async fn revert_contract_state(
        &self,
        to: &BlockIdentifier,
        db: &mut Self::DB,
    ) -> Result<(), StorageError>;
}

pub trait StateGateway<DB>:
    ExtractionStateGateway<DB = DB>
    + ChainGateway<DB = DB>
    + ProtocolGateway<DB = DB>
    + ContractStateGateway<DB = DB>
    + Send
    + Sync
{
}

pub type StateGatewayType<DB, B, TX, T, C> =
    Arc<dyn StateGateway<DB, Transaction = TX, Block = B, Token = T, ContractState = C>>;
