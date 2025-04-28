use lancedb::index::scalar::{
    FtsIndexBuilder as LanceFtsIndexBuilder, TokenizerConfig as LanceTokenizerConfig,
};
use rustler::NifStruct;

#[derive(Debug, NifStruct)]
#[module = "ElixirLanceDB.Native.Table.Index.FTS"]
pub struct FtsIndexBuilderConfig {
    pub with_position: bool,
    pub tokenizer_configs: TokenizerConfig,
}

impl Into<LanceFtsIndexBuilder> for FtsIndexBuilderConfig {
    fn into(self) -> LanceFtsIndexBuilder {
        LanceFtsIndexBuilder {
            with_position: self.with_position,
            tokenizer_configs: self.tokenizer_configs.into(),
        }
    }
}

#[derive(Debug, NifStruct)]
#[module = "ElixirLanceDB.Native.Table.Index.FTS.TokenizerConfig"]
pub struct TokenizerConfig {
    pub base_tokenizer: String,
    pub language: String,
    pub max_token_length: Option<usize>,
    pub lower_case: bool,
    pub stem: bool,
    pub remove_stop_words: bool,
    pub ascii_folding: bool,
}

impl Into<LanceTokenizerConfig> for TokenizerConfig {
    fn into(self) -> LanceTokenizerConfig {
        let mut cfg = LanceTokenizerConfig::default().base_tokenizer(self.base_tokenizer);
        cfg = cfg.clone().language(&self.language).unwrap_or(cfg);

        if let Some(max_token_length) = self.max_token_length {
            cfg = cfg.max_token_length(Some(max_token_length))
        };

        cfg.lower_case(self.lower_case)
            .stem(self.stem)
            .remove_stop_words(self.remove_stop_words)
            .ascii_folding(self.ascii_folding)
    }
}
