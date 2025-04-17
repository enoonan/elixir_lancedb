# defmodule ElixirLanceDB.Native.Shared do
#   alias ElixirLanceDB.Native

#   def from_list(list, {:struct, fields} = dtype, name) when is_list(list) do
#     columns = Map.new(fields, fn {k, _v} -> {k, []} end)

#     columns =
#       Enum.reduce(list, columns, fn
#         nil, columns ->
#           Enum.reduce(fields, columns, fn {field, _}, columns ->
#             Map.update!(columns, field, &[nil | &1])
#           end)

#         row, columns ->
#           Enum.reduce(row, columns, fn {field, value}, columns ->
#             Map.update!(columns, to_string(field), &[value | &1])
#           end)
#       end)

#     series =
#       for {field, inner_dtype} <- fields do
#         columns
#         |> Map.fetch!(field)
#         |> Enum.reverse()
#         |> from_list(inner_dtype, field)
#       end

#     Native.table_from_list_of_series_as_structs(name, series, dtype)
#   end
# end
