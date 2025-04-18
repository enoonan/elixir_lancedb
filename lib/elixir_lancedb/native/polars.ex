defmodule ElixirLanceDB.Native.Polars do
  def from_list(list, dtype), do: from_list(list, dtype, "")

  # def from_list(list, {:list, inner_dtype} = dtype, name) do
  #   series =
  #     Enum.map(list, fn
  #       inner_list when is_list(inner_list) -> from_list(inner_list, inner_dtype, name)
  #       _ -> nil
  #     end)

  #   Native.s_from_list_of_series(name, series, dtype)
  # end

  def from_list(list, {:struct, fields} = dtype, name) when is_list(list) do
    columns = Map.new(fields, fn {k, _v} -> {k, []} end)

    columns =
      Enum.reduce(list, columns, fn
        nil, columns ->
          Enum.reduce(fields, columns, fn {field, _}, columns ->
            Map.update!(columns, field, &[nil | &1])
          end)

        row, columns ->
          Enum.reduce(row, columns, fn {field, value}, columns ->
            Map.update!(columns, to_string(field), &[value | &1])
          end)
      end)

    series =
      for {field, inner_dtype} <- fields do
        columns
        |> Map.fetch!(field)
        |> Enum.reverse()
        |> from_list(inner_dtype, field)
      end

    Native.s_from_list_of_series_as_structs(name, series, dtype)
  end

  def from_list(list, dtype, name) when is_list(list) do
    case dtype do
      # Signed integers
      {:s, 8} -> Native.s_from_list_s8(name, list)
      {:s, 16} -> Native.s_from_list_s16(name, list)
      {:s, 32} -> Native.s_from_list_s32(name, list)
      {:s, 64} -> Native.s_from_list_s64(name, list)
      # Unsigned integers
      {:u, 8} -> Native.s_from_list_u8(name, list)
      {:u, 16} -> Native.s_from_list_u16(name, list)
      {:u, 32} -> Native.s_from_list_u32(name, list)
      {:u, 64} -> Native.s_from_list_u64(name, list)
      # Floats
      {:f, 32} -> Native.s_from_list_f32(name, list)
      {:f, 64} -> Native.s_from_list_f64(name, list)
      :boolean -> Native.s_from_list_bool(name, list)
      :string -> Native.s_from_list_str(name, list)
      :category -> Native.s_from_list_categories(name, list)
      :date -> apply(:s_from_list_date, [name, list])
      :time -> apply(:s_from_list_time, [name, list])
      {:naive_datetime, precision} -> apply(:s_from_list_naive_datetime, [name, list, precision])
      {:datetime, precision, tz} -> apply(:s_from_list_datetime, [name, list, precision, tz])
      {:duration, precision} -> apply(:s_from_list_duration, [name, list, precision])
      :binary -> Native.s_from_list_binary(name, list)
      :null -> Native.s_from_list_null(name, length(list))
      {:decimal, precision, scale} -> apply(:s_from_list_decimal, [name, list, precision, scale])
    end
  end
end
