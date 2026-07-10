use bytes::Bytes;
use crate::provider::{ChunkCost, ProviderAdapter};
use std::sync::Arc;
use tiktoken_rs::CoreBPE;

pub struct AnthropicAdapter;

impl AnthropicAdapter {
    pub fn new(_tokenizer: Arc<CoreBPE>) -> Self {
        Self
    }
}

impl ProviderAdapter for AnthropicAdapter {
    fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
        ChunkCost { estimated_tokens: 0, authoritative_total: None }
    }
}
