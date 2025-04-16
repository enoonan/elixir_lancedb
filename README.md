# ElixirLanceDB

This library aims to bring the fantastic [LanceDB](https://lancedb.github.io/lancedb/) embedded multimodal vector search database to the BEAM.

> 🚧 This project is very new, in active development, and is not published on Hex. 🚧

> 🚧 Feel free to fork, borrow from, or use at your own risk, but do expect the API to change. 🚧


## Phase 1 - Typescript Bridge

In Phase 1 of this project, we access LanceDB's typescript client via [Elixir NodeJS](https://hexdocs.pm/nodejs/readme.html). This involves a couple forms of undesirable overhead:

1. Each call to the typescript client involves serializing and deserializing Elixir structs to JSON to get the data into the JS runtime. The JS runtime then has to reserialize the data to for Rust, which deserializes it yet again.

2. The ElixirLanceDB typescript implementation is quite naive, and doesn't involve maintaining persistent connection. Therefore every call to Lance opens a new DB connection.

However, this is still pretty dang useful especially for smaller workloads. The following functionality is currently supported:

- Create, List, and Drop Tables
- Read, write and update data in tables.
- Perform SQL-like filtering operations 
- Create vector search indices
- Perform vector similarity search with `l2`, `cosine` and `dot` distance types. 

Up next:
- Create full text indices
- Hybrid search

## Phase 2 - Rustler

Since LanceDB is written in Rust, Phase 2 will be to connect directly to Lance using [Rustler](https://github.com/rusterlium/rustler). 

Roadmap:

- [X] Return a DB Connection from LanceDB to Elixir
- [X] List DB tables
- [X] Create DB table from schema
- [X] Drop DB tables
- [ ] Create DB table from data
- [ ] Manage connections using a pool - maybe just for writes
- [ ] Add items to existing table
- [ ] Query items from table
- [ ] Delete items from table
- [ ] Vector search
- [ ] Full text search
- [ ] Hybrid Vector + Full Text search
- [ ] Update table items
- [ ] Modify table schema 
- [ ] Cleanup/Compaction strategies
- [ ] Provide embedding functions to Lance directly

... and ideally as much other functionality as LanceDB Provides.

Having successfully checked off our first couple roadmap items, to the best of my knowledge this is the first time a reference to an open Lance database has been shared to the BEAM. Neat!

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
