defmodule ElixirLanceDB do
  def start_link(opts) do
    DBConnection.start_link(ElixirLanceDB.Connection.Protocol, opts)
  end
end
