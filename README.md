# ElixirLanceDB

This library aims to bring the fantastic [LanceDB](https://lancedb.github.io/lancedb/) to the BEAM using [Rustler](https://github.com/rusterlium/rustler).

Roadmap:

- [ ] Return a DB Connection from LanceDB to Elixir
- [ ] List DB tables
- [ ] Drop DB tables
- [ ] Create DB table from data
- [ ] Create DB table from schema
- [ ] Add items to table
- [ ] Query items from table
- [ ] Delete items from table
- [ ] VECTOR SEARCH 
- [ ] HYBRID SEARCH
- [ ] Update table items
- [ ] Modify table schema 
- [ ] Provide embedding functions to Lance

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
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

