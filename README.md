# ElixirLanceDB

This library aims to bring the fantastic [LanceDB](https://lancedb.github.io/lancedb/) embedded multimodal vector search database to the BEAM. As LanceDB is written in Rust, this integration uses Rustler to map NIFs to the Lance Rust client.

The current focus of this project is to implement an Ecto Adapter so that it can plug into the broader Ecto ecosystem.

> 🚧 This project is very new, in active development, and is not published on Hex. 🚧

> 🚧 Feel free to fork, borrow from, or use at your own risk, but do expect the API to change. 🚧

## Features / Progress

- [X] Return a DB Connection from LanceDB to Elixir
- [X] List DB tables
- [X] Create DB table from schema
- [X] Drop DB tables
- [X] Create DB table from data
- [X] Read data back from table
- [X] Hold peristent connection to table
- [X] List items from table
- [X] Query with SQL filter and limit
- [X] Add items to existing table
- [X] Update items in table
- [X] Delete items from table
- [X] Vector search
- [X] Full text search
- [X] Hybrid Vector + Full Text search
- [X] Optimize table
- [X] Add, drop, and alter table columns
- [ ] Implement an Ecto Adapter
- [ ] Implement an Ash DataLayer
- [ ] Implement table Cleanup/Compaction strategies
- [ ] Implement Lance embedding registry for OpenAI and Sentence Transformers (no need for users to generate own embeddings)
- [ ] Complete various partial implementations - indices, new column configs, data types, table optimization, etc
- [ ] More tests, benchmarks, and documentation
- [ ] Distribute with `rustler_precompiled` 

---

Thanks to:
 * The good folks on the LanceDB discord for guidance and encouraging emojis
 * The good folks on ElixirForum
 * Rustler!
 * Elixir Explorer
 * arrow-elixir
 * snowflake_arrow
 * Polars
 * This convo: https://github.com/jorgecarleitao/arrow2/discussions/1033





<!-- ## Installation -->


<!-- If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `elixir_lancedb` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:elixir_lancedb, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/elixir_lancedb>.
 -->
