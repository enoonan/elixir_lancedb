defmodule ElixirLanceDB.Native.Schema do
  alias ElixirLanceDB.Native.Schema.{Field, Infer}
  @derive Jason.Encoder
  defstruct ~w(fields metadata)a

  @type t() :: %__MODULE__{
          fields: list(atom() | tuple()),
          metadata: map()
        }

  def from(fields, metadata \\ %{}) when is_list(fields) when is_map(metadata) do
    %__MODULE__{
      metadata: metadata,
      fields: fields
    }
  end

  @fsu_min_sample_size 100

  def infer([sample | _] = data, %{} = metadata \\ %{}) do
    {:struct, fields} = sample |> Infer.type()

    {:ok,
     %__MODULE__{
       fields:
         fields
         |> Enum.map(&Field.from_inferred_type/1)
         |> Enum.map(&try_fixed_size_upgrade(&1, data |> Enum.take(@fsu_min_sample_size))),
       metadata: metadata
     }}
  end

  defp try_fixed_size_upgrade(%Field{field_type: {type, _}} = field, _) when type != :list do
    field
  end

  defp try_fixed_size_upgrade(field, sample)
       when length(sample) < @fsu_min_sample_size do
    field
  end

  defp try_fixed_size_upgrade(%Field{field_type: {:list, child_type}} = field, sample) do
    %{name: name} = field
    sample = sample |> ensure_str_keys()
    [hd | _rest] = sample

    if can_upgrade?(sample |> Enum.map(&Map.get(&1, name))) do
      dimension = Map.get(hd, name) |> length

      %Field{
        name: field.name,
        field_type: {:fixed_size_list, child_type, dimension},
        nullable: true
      }
    else
      field
    end
  end

  defp try_fixed_size_upgrade(%Field{} = field, _sample), do: field

  defp can_upgrade?([hd | rest]) do
    hd_len = hd |> length()
    rest |> Enum.all?(&(&1 |> length == hd_len))
  end

  defp ensure_str_keys(list) do
    list
    |> Enum.map(fn item ->
      item |> Enum.map(fn {k, v} -> {k |> to_str_key, v} end) |> Enum.into(%{})
    end)
  end

  defp to_str_key(key) when is_binary(key), do: key
  defp to_str_key(key) when is_atom(key), do: Atom.to_string(key)
end
