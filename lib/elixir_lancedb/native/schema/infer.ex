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
  def type(%Date{} = _item), do: :date
  def type(%Time{} = _item), do: :time
  def type(%DateTime{time_zone: tz} = _item), do: {:datetime, :microsecond, tz}
  def type(%NaiveDateTime{} = _item), do: {:naive_datetime, :microsecond}

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

  defp to_str_key(key) when is_binary(key), do: key
  defp to_str_key(key) when is_atom(key), do: Atom.to_string(key)
end
