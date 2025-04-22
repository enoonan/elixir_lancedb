use crate::atoms;

use super::DistanceType;
use lancedb::index::vector::IvfPqIndexBuilder;
use rustler::{Decoder, NifResult, Term};

#[derive(Debug)]
pub struct IvfPqIndexBuilderConfig {
    // IVF
    pub distance_type: DistanceType,
    pub num_partitions: Option<u32>,
    pub sample_rate: u32,
    pub max_iterations: u32,

    // PQ
    pub num_sub_vectors: Option<u32>,
    pub num_bits: Option<u32>,
}

impl Into<IvfPqIndexBuilder> for IvfPqIndexBuilderConfig {
    fn into(self) -> IvfPqIndexBuilder {
        let mut builder = IvfPqIndexBuilder::default();
        builder = builder
            .distance_type(self.distance_type.into())
            .max_iterations(self.max_iterations)
            .sample_rate(self.sample_rate);

        if let Some(num_bits) = self.num_bits {
            builder = builder.num_bits(num_bits);
        }

        if let Some(num_partitions) = self.num_partitions {
            builder = builder.num_partitions(num_partitions);
        }

        if let Some(num_sub_vectors) = self.num_sub_vectors {
            builder = builder.num_sub_vectors(num_sub_vectors);
        }

        builder
    }
}

impl Decoder<'_> for IvfPqIndexBuilderConfig {
    fn decode(term: Term<'_>) -> NifResult<Self> {
        let distance_type_atom_str = term
            .map_get(atoms::distance_index_type())?
            .atom_to_string()?;

        let distance_type = match distance_type_atom_str.as_str() {
            "l2" => Ok(DistanceType::L2),
            "cosine" => Ok(DistanceType::Cosine),
            "dot" => Ok(DistanceType::Dot),
            "hamming" => Ok(DistanceType::Hamming),
            _ => Err(rustler::Error::BadArg),
        }?;

        let num_partitions: Option<u32> = term
            .map_get(atoms::num_partitions())
            .ok()
            .and_then(|s| s.decode().ok());

        let sample_rate: u32 = term.map_get(atoms::sample_rate())?.decode()?;
        let max_iterations: u32 = term.map_get(atoms::max_iterations())?.decode()?;

        let num_sub_vectors: Option<u32> = term
            .map_get(atoms::num_subvectors())
            .ok()
            .and_then(|s| s.decode().ok());

        let num_bits: Option<u32> = term
            .map_get(atoms::num_bits())
            .ok()
            .and_then(|s| s.decode().ok());

        Ok(IvfPqIndexBuilderConfig {
            distance_type,
            num_partitions,
            sample_rate,
            max_iterations,
            num_sub_vectors,
            num_bits,
        })
    }
}
