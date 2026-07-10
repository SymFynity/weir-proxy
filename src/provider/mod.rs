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

    /// Parses a complete (non-streaming) JSON response body and returns the
    /// authoritative total token count, if the body's shape is recognized.
    /// Non-streaming responses always carry their own authoritative usage
    /// (no interim estimation needed, unlike the streaming `chunk_cost`
    /// path). Returns `None` for a body that doesn't parse as an expected
    /// response shape (e.g. a provider error response) — no usage means
    /// nothing to charge.
    fn non_streaming_cost(&self, body: &Bytes) -> Option<u64>;
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
