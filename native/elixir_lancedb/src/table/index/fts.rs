use lancedb::index::scalar::{
    FtsIndexBuilder as LanceFtsIndexBuilder, TokenizerConfig as LanceTokenizerConfig,
};
use rustler::{Decoder, NifResult, Term};

use crate::atoms;

#[derive(Debug)]
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

impl Decoder<'_> for FtsIndexBuilderConfig {
    fn decode(term: rustler::Term<'_>) -> NifResult<Self> {
        let with_position = term.map_get(atoms::with_position())?.decode::<bool>()?;
        let tokenizer_configs = term
            .map_get(atoms::tokenizer_configs())?
            .decode::<TokenizerConfig>()?;

        Ok(FtsIndexBuilderConfig {
            with_position,
            tokenizer_configs,
        })
    }
}

#[derive(Debug)]
pub struct TokenizerConfig {
    pub base_tokenizer: String,
    pub language: String,
    pub max_token_length: Option<usize>,
    pub lower_case: bool,
    pub stem: bool,
    pub remove_stop_words: bool,
    pub ascii_folding: bool,
}

impl Decoder<'_> for TokenizerConfig {
    fn decode(term: Term<'_>) -> NifResult<Self> {
        let base_tokenizer: String = term.map_get(atoms::base_tokenizer())?.atom_to_string()?;
        let language: String = term.map_get(atoms::language())?.atom_to_string()?;
        let max_token_length: Option<usize> = term
            .map_get(atoms::max_token_length())
            .ok()
            .and_then(|s| s.decode().ok());
        let lower_case: bool = term.map_get(atoms::lower_case())?.decode()?;
        let stem: bool = term.map_get(atoms::stem())?.decode()?;
        let remove_stop_words: bool = term.map_get(atoms::remove_stop_words())?.decode()?;
        let ascii_folding: bool = term.map_get(atoms::ascii_folding())?.decode()?;

        Ok(TokenizerConfig {
            base_tokenizer,
            language,
            max_token_length,
            lower_case,
            stem,
            remove_stop_words,
            ascii_folding,
        })
    }
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
