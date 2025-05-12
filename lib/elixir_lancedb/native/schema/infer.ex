defmodule ElixirLanceDB.Native.Schema.Infer do
  # Primitive types
  def type(field) when is_nil(field), do: :null
  def type(field) when is_binary(field), do: :utf8
  def type(field) when is_integer(field), do: :int32
  def type(field) when is_float(field), do: :float32

  # Lists
  def type([]), do: raise("Cannot infer field type of empty list")

  def type([hd | _]), do: {:list, hd |> type}

  # Datetimes
  def type(%Date{} = _item), do: :date32
  def type(%Time{}), do: {:time32, :millisecond}
  def type(%DateTime{} = _dt), do: :date64
  def type(%NaiveDateTime{} = _item), do: :date64

  # Map

  def type(%{} = map) do
    types =
      for {key, value} <- map |> Map.new() do
        key = key |> to_str_key()

        case value do
          v when not is_map(v) -> {key, v |> type}
          v -> {key, v |> type}
        end
      end

    {:struct, Enum.sort(types)}
  end

  def needs_cleaning?(initial_data) do
    initial_data
    |> Enum.take(100)
    |> Enum.any?(fn record ->
      record
      |> Map.to_list()
      |> Enum.any?(fn {_k, v} ->
        case v do
          %Date{} -> true
          %Time{} -> true
          %DateTime{} -> true
          %NaiveDateTime{} -> true
          _ -> false
        end
      end)
    end)
  end

  def clean(initial_data) do
    initial_data
    |> Enum.map(fn record ->
      record
      |> Map.to_list()
      |> Enum.map(fn {k, val} ->
        case val do
          %Date{} = v ->
            dt = DateTime.new!(v, Time.new!(0, 0, 0))
            {k, dt.microsecond}

          %Time{} = v ->
            {k, v.microsecond * 1000}

          %DateTime{} = v ->
            {k, DateTime.to_unix(v, :millisecond)}

          %NaiveDateTime{} = v ->
            {k, DateTime.from_naive(v, "+00:00") |> DateTime.to_unix(:millisecond)}

          _ ->
            {k, val}
        end
      end)
      |> Map.new()
    end)
  end

  defp to_str_key(key) when is_binary(key), do: key
  defp to_str_key(key) when is_atom(key), do: Atom.to_string(key)
end
