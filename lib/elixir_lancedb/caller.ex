defmodule ElixirLanceDB.Caller do
  def call(func, params) when is_list(params) do
    NodeJS.call({"src", func}, [uri()] ++ params)
  end

  def call(func, params), do: call(func, [params])

  def call(func), do: call(func, [])

  defp uri(), do: Application.get_env(:elixir_lancedb, :data_dir)
end
