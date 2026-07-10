use std::sync::Arc;
use bytes::Bytes;
use serde::Deserialize;
use tiktoken_rs::CoreBPE;

use crate::provider::{ChunkCost, ProviderAdapter};

pub struct AnthropicAdapter {
    tokenizer: Arc<CoreBPE>,
    input_tokens: u64,
}

impl AnthropicAdapter {
    pub fn new(tokenizer: Arc<CoreBPE>) -> Self {
        Self { tokenizer, input_tokens: 0 }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum AnthropicEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: AnthropicMessageStart },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { delta: AnthropicDelta },
    #[serde(rename = "message_delta")]
    MessageDelta { usage: AnthropicOutputUsage },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct AnthropicMessageStart {
    usage: AnthropicInputUsage,
}

#[derive(Deserialize)]
struct AnthropicInputUsage {
    input_tokens: u64,
}

#[derive(Deserialize, Default)]
struct AnthropicDelta {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Deserialize)]
struct AnthropicOutputUsage {
    output_tokens: u64,
}

impl ProviderAdapter for AnthropicAdapter {
    fn chunk_cost(&mut self, raw: &Bytes) -> ChunkCost {
        let mut estimated_tokens = 0u64;
        let mut authoritative_total = None;
        let text = String::from_utf8_lossy(raw);

        for line in text.lines() {
            let Some(payload) = line.strip_prefix("data: ") else { continue };
            let Ok(event) = serde_json::from_str::<AnthropicEvent>(payload) else { continue };

            match event {
                AnthropicEvent::MessageStart { message } => {
                    self.input_tokens = message.usage.input_tokens;
                    authoritative_total = Some(self.input_tokens);
                }
                AnthropicEvent::ContentBlockDelta { delta } => {
                    if let Some(t) = delta.text {
                        estimated_tokens += self.tokenizer.encode_ordinary(&t).len() as u64;
                    }
                }
                AnthropicEvent::MessageDelta { usage } => {
                    authoritative_total = Some(self.input_tokens + usage.output_tokens);
                }
                AnthropicEvent::Other => {}
            }
        }

        ChunkCost { estimated_tokens, authoritative_total }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenizer() -> Arc<CoreBPE> {
        Arc::new(tiktoken_rs::cl100k_base().unwrap())
    }

    #[test]
    fn message_start_sets_authoritative_baseline() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.authoritative_total, Some(25));
    }

    #[test]
    fn content_block_delta_contributes_estimate() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert!(cost.estimated_tokens >= 1);
        assert_eq!(cost.authoritative_total, None);
    }

    #[test]
    fn message_delta_combines_input_baseline_with_output_tokens() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let start = Bytes::from_static(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n",
        );
        adapter.chunk_cost(&start);

        let delta = Bytes::from_static(
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":15}}\n\n",
        );
        let cost = adapter.chunk_cost(&delta);
        assert_eq!(cost.authoritative_total, Some(40)); // 25 input + 15 output
    }
}
