use anyhow::{anyhow, bail};
use ethabi::{decode, ParamType};
use substreams_ethereum::pb::eth::v2::Call;
use tiny_keccak::{Hasher, Keccak};

pub fn encode_pool_hash(token_x: Vec<u8>, token_y: Vec<u8>, pool_idx: Vec<u8>) -> [u8; 32] {
    let mut keccak = Keccak::v256();

    let mut data = Vec::new();
    data.extend_from_slice(&token_x);
    data.extend_from_slice(&token_y);
    data.extend_from_slice(&pool_idx);

    let mut output = [0u8; 32];
    keccak.update(&data);
    keccak.finalize(&mut output);
    output
}

pub fn decode_flows_from_output(call: &Call) -> Result<(ethabi::Int, ethabi::Int), anyhow::Error> {
    if let Ok(external_outputs) = decode(BASE_QUOTE_FLOW_OUTPUT, &call.return_data) {
        let base_flow = external_outputs[0]
            .to_owned()
            .into_int() // Needs conversion into bytes for next step
            .ok_or_else(|| anyhow!("Failed to convert base flow to i128".to_string()))?;

        let quote_flow = external_outputs[1]
            .to_owned()
            .into_int() // Needs conversion into bytes for next step
            .ok_or_else(|| anyhow!("Failed to convert quote flow to i128".to_string()))?;
        Ok((base_flow, quote_flow))
    } else {
        bail!("Failed to decode swap call outputs.".to_string());
    }
}

const BASE_QUOTE_FLOW_OUTPUT: &[ParamType] = &[
    // The token base and quote token flows associated with this swap action.
    // Negative indicates a credit paid to the user (token balance of pool
    // decreases), positive a debit collected from the user (token balance of pool
    // increases).
    ParamType::Int(128), // baseFlow
    ParamType::Int(128), // quoteFlow
];
