use bytes::Bytes;

use rust_data_distribution::{InstrumentMode, PoolParser, RegisterTokenPayload};

fn dummy_parser(_bytes: Bytes) -> (i64, i64) {
    (1, 1)
}

fn main() {
    let mut token_to_shard: Vec<RegisterTokenPayload> = Vec::new();
    let entry_1 = RegisterTokenPayload {
        token: 123456,
        shard: 1_usize,
        mode: InstrumentMode::Ring,
    };

    token_to_shard.push(entry_1);
    let _parser_pool = PoolParser::new(8, dummy_parser, token_to_shard);
    println!("Hello, world!");
}
