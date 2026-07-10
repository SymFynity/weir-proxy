use std::sync::Arc;
use bytes::Bytes;
use serde::Deserialize;
use tiktoken_rs::CoreBPE;

use crate::provider::{ChunkCost, ProviderAdapter};

pub struct OpenAiAdapter {
    tokenizer: Arc<CoreBPE>,
}

impl OpenAiAdapter {
    pub fn new(tokenizer: Arc<CoreBPE>) -> Self {
        Self { tokenizer }
    }
}

#[derive(Deserialize)]
struct OpenAiChunk {
    #[serde(default)]
    choices: Vec<OpenAiChoice>,
    usage: Option<OpenAiUsage>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    delta: OpenAiDelta,
}

#[derive(Deserialize, Default)]
struct OpenAiDelta {
    content: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiUsage {
    total_tokens: u64,
}

impl ProviderAdapter for OpenAiAdapter {
    fn chunk_cost(&mut self, raw: &Bytes) -> ChunkCost {
        let mut estimated_tokens = 0u64;
        let mut authoritative_total = None;
        let text = String::from_utf8_lossy(raw);

        for line in text.lines() {
            let Some(payload) = line.strip_prefix("data: ") else { continue };
            if payload.trim() == "[DONE]" {
                continue;
            }
            let Ok(chunk) = serde_json::from_str::<OpenAiChunk>(payload) else { continue };

            for choice in &chunk.choices {
                if let Some(content) = &choice.delta.content {
                    estimated_tokens += self.tokenizer.encode_ordinary(content).len() as u64;
                }
            }
            if let Some(usage) = chunk.usage {
                authoritative_total = Some(usage.total_tokens);
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
    fn estimates_tokens_from_content_delta() {
        let mut adapter = OpenAiAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert!(cost.estimated_tokens >= 1);
        assert_eq!(cost.authoritative_total, None);
    }

    #[test]
    fn extracts_authoritative_usage_from_final_chunk() {
        let mut adapter = OpenAiAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.authoritative_total, Some(7));
    }

    #[test]
    fn ignores_done_sentinel() {
        let mut adapter = OpenAiAdapter::new(tokenizer());
        let raw = Bytes::from_static(b"data: [DONE]\n\n");
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.estimated_tokens, 0);
        assert_eq!(cost.authoritative_total, None);
    }

    #[test]
    fn skips_malformed_json_lines_without_panicking() {
        let mut adapter = OpenAiAdapter::new(tokenizer());
        let raw = Bytes::from_static(b"data: {not valid json\n\n");
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.estimated_tokens, 0);
        assert_eq!(cost.authoritative_total, None);
    }

    #[test]
    fn handles_non_utf8_bytes_without_panicking() {
        let mut adapter = OpenAiAdapter::new(tokenizer());
        let mut raw = b"data: {\"choices\":[{\"delta\":{\"content\":\"".to_vec();
        raw.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        raw.extend_from_slice(b"\"}}]}\n\n");
        let cost = adapter.chunk_cost(&Bytes::from(raw));
        // Must not panic; invalid UTF-8 is lossily replaced, so the line may or
        // may not parse as valid JSON depending on replacement characters, but
        // either way chunk_cost must return normally.
        let _ = cost;
    }
}
