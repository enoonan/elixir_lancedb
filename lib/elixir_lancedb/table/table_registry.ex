defmodule ElixirLanceDB.Table.TableRegistry do
  # def child_spec(opts) do
  #   %{
  #     id: __MODULE__,
  #     start: {__MODULE__, :start_link, [opts]},
  #     type: :worker,
  #     restart: :permanent,
  #     shutdown: 500
  #   }
  # end

  def start_link(opts) do
    Registry.start_link(opts)
  end

  def via_tuple(site_id) do
    {:via, Registry, {__MODULE__, site_id}}
  end
end
