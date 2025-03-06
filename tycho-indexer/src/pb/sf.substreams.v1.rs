// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoreDeltas {
    #[prost(message, repeated, tag="1")]
    pub store_deltas: ::prost::alloc::vec::Vec<StoreDelta>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoreDelta {
    #[prost(enumeration="store_delta::Operation", tag="1")]
    pub operation: i32,
    #[prost(uint64, tag="2")]
    pub ordinal: u64,
    #[prost(string, tag="3")]
    pub key: ::prost::alloc::string::String,
    #[prost(bytes="vec", tag="4")]
    pub old_value: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="5")]
    pub new_value: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `StoreDelta`.
pub mod store_delta {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Operation {
        Unset = 0,
        Create = 1,
        Update = 2,
        Delete = 3,
    }
    impl Operation {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Operation::Unset => "UNSET",
                Operation::Create => "CREATE",
                Operation::Update => "UPDATE",
                Operation::Delete => "DELETE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNSET" => Some(Self::Unset),
                "CREATE" => Some(Self::Create),
                "UPDATE" => Some(Self::Update),
                "DELETE" => Some(Self::Delete),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Modules {
    #[prost(message, repeated, tag="1")]
    pub modules: ::prost::alloc::vec::Vec<Module>,
    #[prost(message, repeated, tag="2")]
    pub binaries: ::prost::alloc::vec::Vec<Binary>,
}
/// Binary represents some code compiled to its binary form.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Binary {
    #[prost(string, tag="1")]
    pub r#type: ::prost::alloc::string::String,
    #[prost(bytes="vec", tag="2")]
    pub content: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Module {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint32, tag="4")]
    pub binary_index: u32,
    #[prost(string, tag="5")]
    pub binary_entrypoint: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="6")]
    pub inputs: ::prost::alloc::vec::Vec<module::Input>,
    #[prost(message, optional, tag="7")]
    pub output: ::core::option::Option<module::Output>,
    #[prost(uint64, tag="8")]
    pub initial_block: u64,
    #[prost(message, optional, tag="9")]
    pub block_filter: ::core::option::Option<module::BlockFilter>,
    #[prost(oneof="module::Kind", tags="2, 3, 10")]
    pub kind: ::core::option::Option<module::Kind>,
}
/// Nested message and enum types in `Module`.
pub mod module {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BlockFilter {
        #[prost(string, tag="1")]
        pub module: ::prost::alloc::string::String,
        #[prost(oneof="block_filter::Query", tags="2, 3")]
        pub query: ::core::option::Option<block_filter::Query>,
    }
    /// Nested message and enum types in `BlockFilter`.
    pub mod block_filter {
        #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Query {
            #[prost(string, tag="2")]
            QueryString(::prost::alloc::string::String),
            /// QueryFromStore query_from_store_keys = 3;
            #[prost(message, tag="3")]
            QueryFromParams(super::QueryFromParams),
        }
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
    pub struct QueryFromParams {
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
    pub struct KindMap {
        #[prost(string, tag="1")]
        pub output_type: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
    pub struct KindStore {
        /// The `update_policy` determines the functions available to mutate the store
        /// (like `set()`, `set_if_not_exists()` or `sum()`, etc..) in
        /// order to ensure that parallel operations are possible and deterministic
        ///
        /// Say a store cumulates keys from block 0 to 1M, and a second store
        /// cumulates keys from block 1M to 2M. When we want to use this
        /// store as a dependency for a downstream module, we will merge the
        /// two stores according to this policy.
        #[prost(enumeration="kind_store::UpdatePolicy", tag="1")]
        pub update_policy: i32,
        #[prost(string, tag="2")]
        pub value_type: ::prost::alloc::string::String,
    }
    /// Nested message and enum types in `KindStore`.
    pub mod kind_store {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum UpdatePolicy {
            Unset = 0,
            /// Provides a store where you can `set()` keys, and the latest key wins
            Set = 1,
            /// Provides a store where you can `set_if_not_exists()` keys, and the first key wins
            SetIfNotExists = 2,
            /// Provides a store where you can `add_*()` keys, where two stores merge by summing its values.
            Add = 3,
            /// Provides a store where you can `min_*()` keys, where two stores merge by leaving the minimum value.
            Min = 4,
            /// Provides a store where you can `max_*()` keys, where two stores merge by leaving the maximum value.
            Max = 5,
            /// Provides a store where you can `append()` keys, where two stores merge by concatenating the bytes in order.
            Append = 6,
            /// Provides a store with both `set()` and `sum()` functions.
            SetSum = 7,
        }
        impl UpdatePolicy {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    UpdatePolicy::Unset => "UPDATE_POLICY_UNSET",
                    UpdatePolicy::Set => "UPDATE_POLICY_SET",
                    UpdatePolicy::SetIfNotExists => "UPDATE_POLICY_SET_IF_NOT_EXISTS",
                    UpdatePolicy::Add => "UPDATE_POLICY_ADD",
                    UpdatePolicy::Min => "UPDATE_POLICY_MIN",
                    UpdatePolicy::Max => "UPDATE_POLICY_MAX",
                    UpdatePolicy::Append => "UPDATE_POLICY_APPEND",
                    UpdatePolicy::SetSum => "UPDATE_POLICY_SET_SUM",
                }
            }
            /// Creates an enum from field names used in the ProtoBuf definition.
            pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                match value {
                    "UPDATE_POLICY_UNSET" => Some(Self::Unset),
                    "UPDATE_POLICY_SET" => Some(Self::Set),
                    "UPDATE_POLICY_SET_IF_NOT_EXISTS" => Some(Self::SetIfNotExists),
                    "UPDATE_POLICY_ADD" => Some(Self::Add),
                    "UPDATE_POLICY_MIN" => Some(Self::Min),
                    "UPDATE_POLICY_MAX" => Some(Self::Max),
                    "UPDATE_POLICY_APPEND" => Some(Self::Append),
                    "UPDATE_POLICY_SET_SUM" => Some(Self::SetSum),
                    _ => None,
                }
            }
        }
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
    pub struct KindBlockIndex {
        #[prost(string, tag="1")]
        pub output_type: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Input {
        #[prost(oneof="input::Input", tags="1, 2, 3, 4")]
        pub input: ::core::option::Option<input::Input>,
    }
    /// Nested message and enum types in `Input`.
    pub mod input {
        #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Source {
            /// ex: "sf.ethereum.type.v1.Block"
            #[prost(string, tag="1")]
            pub r#type: ::prost::alloc::string::String,
        }
        #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Map {
            /// ex: "block_to_pairs"
            #[prost(string, tag="1")]
            pub module_name: ::prost::alloc::string::String,
        }
        #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Store {
            #[prost(string, tag="1")]
            pub module_name: ::prost::alloc::string::String,
            #[prost(enumeration="store::Mode", tag="2")]
            pub mode: i32,
        }
        /// Nested message and enum types in `Store`.
        pub mod store {
            #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
            #[repr(i32)]
            pub enum Mode {
                Unset = 0,
                Get = 1,
                Deltas = 2,
            }
            impl Mode {
                /// String value of the enum field names used in the ProtoBuf definition.
                ///
                /// The values are not transformed in any way and thus are considered stable
                /// (if the ProtoBuf definition does not change) and safe for programmatic use.
                pub fn as_str_name(&self) -> &'static str {
                    match self {
                        Mode::Unset => "UNSET",
                        Mode::Get => "GET",
                        Mode::Deltas => "DELTAS",
                    }
                }
                /// Creates an enum from field names used in the ProtoBuf definition.
                pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
                    match value {
                        "UNSET" => Some(Self::Unset),
                        "GET" => Some(Self::Get),
                        "DELTAS" => Some(Self::Deltas),
                        _ => None,
                    }
                }
            }
        }
        #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Params {
            #[prost(string, tag="1")]
            pub value: ::prost::alloc::string::String,
        }
        #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Input {
            #[prost(message, tag="1")]
            Source(Source),
            #[prost(message, tag="2")]
            Map(Map),
            #[prost(message, tag="3")]
            Store(Store),
            #[prost(message, tag="4")]
            Params(Params),
        }
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Output {
        #[prost(string, tag="1")]
        pub r#type: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag="2")]
        KindMap(KindMap),
        #[prost(message, tag="3")]
        KindStore(KindStore),
        #[prost(message, tag="10")]
        KindBlockIndex(KindBlockIndex),
    }
}
/// Clock is a pointer to a block with added timestamp
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Clock {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub number: u64,
    #[prost(message, optional, tag="3")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
}
/// BlockRef is a pointer to a block to which we don't know the timestamp
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockRef {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub number: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Package {
    /// Needs to be one so this file can be used _directly_ as a
    /// buf `Image` andor a ProtoSet for grpcurl and other tools
    #[prost(message, repeated, tag="1")]
    pub proto_files: ::prost::alloc::vec::Vec<::prost_types::FileDescriptorProto>,
    #[prost(uint64, tag="5")]
    pub version: u64,
    #[prost(message, optional, tag="6")]
    pub modules: ::core::option::Option<Modules>,
    #[prost(message, repeated, tag="7")]
    pub module_meta: ::prost::alloc::vec::Vec<ModuleMetadata>,
    #[prost(message, repeated, tag="8")]
    pub package_meta: ::prost::alloc::vec::Vec<PackageMetadata>,
    /// Source network for Substreams to fetch its data from.
    #[prost(string, tag="9")]
    pub network: ::prost::alloc::string::String,
    #[prost(message, optional, tag="10")]
    pub sink_config: ::core::option::Option<::prost_types::Any>,
    #[prost(string, tag="11")]
    pub sink_module: ::prost::alloc::string::String,
    /// image is the bytes to a JPEG, WebP or PNG file. Max size is 2 MiB
    #[prost(bytes="vec", tag="12")]
    pub image: ::prost::alloc::vec::Vec<u8>,
    #[prost(map="string, message", tag="13")]
    pub networks: ::std::collections::HashMap<::prost::alloc::string::String, NetworkParams>,
    #[prost(map="string, string", tag="14")]
    pub block_filters: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NetworkParams {
    #[prost(map="string, uint64", tag="1")]
    pub initial_blocks: ::std::collections::HashMap<::prost::alloc::string::String, u64>,
    #[prost(map="string, string", tag="2")]
    pub params: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PackageMetadata {
    #[prost(string, tag="1")]
    pub version: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub url: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub doc: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub description: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModuleMetadata {
    /// Corresponds to the index in `Package.metadata.package_meta`
    #[prost(uint64, tag="1")]
    pub package_index: u64,
    #[prost(string, tag="2")]
    pub doc: ::prost::alloc::string::String,
}
// @@protoc_insertion_point(module)
