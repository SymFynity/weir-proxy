pub mod openai;
pub mod anthropic;

use std::sync::Arc;
use bytes::Bytes;
use tiktoken_rs::CoreBPE;

pub use openai::OpenAiAdapter;
pub use anthropic::AnthropicAdapter;

pub struct ChunkCost {
    pub estimated_tokens: u64,
    pub authoritative_total: Option<u64>,
}

pub trait ProviderAdapter: Send {
    fn chunk_cost(&mut self, raw: &Bytes) -> ChunkCost;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    OpenAi,
    Anthropic,
}

pub struct Tokenizer(Arc<CoreBPE>);

impl Tokenizer {
    pub fn load() -> Self {
        Self(Arc::new(tiktoken_rs::cl100k_base().expect("bundled cl100k tokenizer")))
    }

    pub fn new_adapter(&self, provider: Provider) -> Box<dyn ProviderAdapter> {
        match provider {
            Provider::OpenAi => Box::new(OpenAiAdapter::new(self.0.clone())),
            Provider::Anthropic => Box::new(AnthropicAdapter::new(self.0.clone())),
        }
    }
}
