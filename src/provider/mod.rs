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
    /// Names of any tools invoked in this chunk/event — never call
    /// arguments, only the tool's name, per the project's privacy line.
    pub tool_calls: Vec<String>,
}

/// The result of parsing a complete (non-streaming) JSON response body.
pub struct NonStreamingCost {
    pub total_tokens: Option<u64>,
    /// Names of any tools invoked anywhere in the response — never call
    /// arguments.
    pub tool_calls: Vec<String>,
}

pub trait ProviderAdapter: Send {
    fn chunk_cost(&mut self, raw: &Bytes) -> ChunkCost;

    /// Parses a complete (non-streaming) JSON response body and returns
    /// its authoritative total token count (if present) and any tool
    /// calls found. Non-streaming responses always carry their own
    /// authoritative usage (no interim estimation needed, unlike the
    /// streaming `chunk_cost` path).
    fn non_streaming_cost(&self, body: &Bytes) -> NonStreamingCost;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
