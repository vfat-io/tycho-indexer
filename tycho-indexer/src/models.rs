#[allow(unused_imports)]
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

use crate::extractor::{evm::Transaction, ExtractionError};
use strum_macros::{Display, EnumString};

use crate::{hex_bytes::Bytes, pb::tycho::evm::v1 as substreams};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumString, Display, Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Chain {
    #[default]
    Ethereum,
    Starknet,
    ZkSync,
}

#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub enum ProtocolSystem {
    Ambient,
}

#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub enum ImplementationType {
    Vm,
    Custom,
}

#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub enum FinancialType {
    Swap,
    Lend,
    Leverage,
    Psm,
}

#[allow(dead_code)]
#[derive(PartialEq, Debug)]
pub struct ProtocolType {
    name: String,
    attribute_schema: serde_json::Value,
    financial_type: FinancialType,
    implementation_type: ImplementationType,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ExtractorIdentity {
    pub chain: Chain,
    pub name: String,
}

impl ExtractorIdentity {
    pub fn new(chain: Chain, name: &str) -> Self {
        Self { chain, name: name.to_owned() }
    }
}

impl std::fmt::Display for ExtractorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.chain, self.name)
    }
}

#[derive(Debug, PartialEq)]
pub struct ExtractionState {
    pub name: String,
    pub chain: Chain,
    pub attributes: serde_json::Value,
    pub cursor: Vec<u8>,
}

impl ExtractionState {
    pub fn new(
        name: String,
        chain: Chain,
        attributes: Option<serde_json::Value>,
        cursor: &[u8],
    ) -> Self {
        ExtractionState {
            name,
            chain,
            attributes: attributes.unwrap_or_default(),
            cursor: cursor.to_vec(),
        }
    }
}

#[typetag::serde(tag = "type")]
pub trait NormalisedMessage: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static {
    fn source(&self) -> ExtractorIdentity;
}

#[allow(dead_code)]
pub struct ProtocolComponent<T> {
    // an id for this component, could be hex repr of contract address
    id: String,
    // what system this component belongs to
    protocol_system: ProtocolSystem,
    // more metadata information about the components general type (swap, lend, bridge, etc.)
    protocol_type: ProtocolType,
    // Blockchain the component belongs to
    chain: Chain,
    // holds the tokens tradable
    tokens: Vec<T>,
    // ID's referring to related contracts
    contract_ids: Vec<String>,
    // allows to express some validation over the attributes if necessary
    attribute_schema: Bytes,
}

impl ProtocolComponent<String> {
    #[allow(dead_code)]
    pub fn try_from_message(
        msg: substreams::ProtocolComponent,
        protocol_system: ProtocolSystem,
        protocol_type: ProtocolType,
        chain: Chain,
    ) -> Result<Self, ExtractionError> {
        let id = String::from_utf8(msg.id)
            .map_err(|error| ExtractionError::DecodeError(error.to_string()))
            .unwrap();
        let tokens = msg
            .tokens
            .into_iter()
            .map(|t| {
                String::from_utf8(t)
                    .map_err(|error| ExtractionError::DecodeError(error.to_string()))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let contract_ids = msg
            .contracts
            .into_iter()
            .map(|contract_id| {
                String::from_utf8(contract_id)
                    .map_err(|error| ExtractionError::DecodeError(error.to_string()))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        Ok(Self {
            id,
            protocol_type,
            protocol_system,
            tokens,
            contract_ids,
            attribute_schema: Bytes::default(),
            chain,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TvlChange<T> {
    token: T,
    new_balance: f64,
    // tx where the this balance was observed
    tx: String,
}

impl TvlChange<String> {
    #[allow(dead_code)]
    pub fn try_from_message(
        msg: substreams::TvlUpdate,
        tx: &Transaction,
    ) -> Result<Self, ExtractionError> {
        Ok(Self {
            token: String::from_utf8(msg.token)
                .map_err(|error| ExtractionError::DecodeError(error.to_string()))?,
            new_balance: f64::from_bits(u64::from_le_bytes(msg.balance.try_into().unwrap())),
            tx: tx.hash.to_string(),
        })
    }
}

#[allow(dead_code)]
pub struct ProtocolState {
    // associates the back to a component, which has metadata like type, tokens , etc.
    component_id: String,
    // holds all the protocol specific attributes, validates by the components schema
    attributes: HashMap<String, Bytes>,
    // via transaction, we can trace back when this state became valid
    modify_tx: Bytes,
}

#[cfg(test)]
mod test {
    use super::*;
    #[allow(unused_imports)]
    use actix_web::body::MessageBody;
    use ethers::types::{H160, H256};
    use rstest::rstest;

    fn create_transaction() -> Transaction {
        Transaction {
            hash: H256::from_low_u64_be(
                0x0000000000000000000000000000000000000000000000000000000011121314,
            ),
            block_hash: H256::from_low_u64_be(
                0x0000000000000000000000000000000000000000000000000000000031323334,
            ),
            from: H160::from_low_u64_be(0x0000000000000000000000000000000041424344),
            to: Some(H160::from_low_u64_be(0x0000000000000000000000000000000051525354)),
            index: 2,
        }
    }

    #[rstest]
    fn test_try_from_message_protocol_component() {
        // Sample data for testing
        let msg = substreams::ProtocolComponent {
            id: b"component_id".to_vec(),
            tokens: vec![b"token1".to_vec(), b"token2".to_vec()],
            contracts: vec![b"contract1".to_vec(), b"contract2".to_vec()],
        };

        // Sample parameters for testing
        let protocol_system = ProtocolSystem::Ambient;
        let protocol_type = ProtocolType {
            name: "Pool".to_string(),
            attribute_schema: serde_json::Value::default(),
            financial_type: crate::models::FinancialType::Psm,
            implementation_type: crate::models::ImplementationType::Custom,
        };
        let chain = Chain::Ethereum;

        // Call the try_from_message method
        let result = ProtocolComponent::<String>::try_from_message(
            msg,
            ProtocolSystem::Ambient,
            ProtocolType {
                name: "Pool".to_string(),
                attribute_schema: serde_json::Value::default(),
                financial_type: crate::models::FinancialType::Psm,
                implementation_type: crate::models::ImplementationType::Custom,
            },
            Chain::Ethereum,
        );

        // Assert the result
        assert!(result.is_ok());

        // Unwrap the result for further assertions
        let protocol_component = result.unwrap();

        // Assert specific properties of the protocol component
        assert_eq!(protocol_component.id, "component_id");
        assert_eq!(protocol_component.protocol_system, protocol_system);
        assert_eq!(protocol_component.protocol_type, protocol_type);
        assert_eq!(protocol_component.chain, chain);
        assert_eq!(protocol_component.tokens, vec!["token1".to_string(), "token2".to_string()]);
        assert_eq!(
            protocol_component.contract_ids,
            vec!["contract1".to_string(), "contract2".to_string()]
        );
        assert_eq!(protocol_component.attribute_schema, Bytes::default());
    }

    #[rstest]
    fn test_try_from_message_tvl_change() {
        let tx = create_transaction();
        let expected_balance: f64 = 3000.0;
        let msg_balance = expected_balance.to_le_bytes().to_vec();

        let expected_token = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
        let msg_token = expected_token
            .try_into_bytes()
            .unwrap()
            .to_vec();

        let msg = substreams::TvlUpdate {
            balance: msg_balance.to_vec(),
            token: msg_token,
            component_id: Vec::default(),
        };
        let from_message = TvlChange::try_from_message(msg, &tx).unwrap();

        assert_eq!(from_message.new_balance, expected_balance);
        assert_eq!(from_message.tx, tx.hash.to_string());
        assert_eq!(from_message.token, expected_token);
    }
}
