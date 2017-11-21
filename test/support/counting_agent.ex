defmodule Sage.CounterAgent do
  @moduledoc false
  def start_link() do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def start_from(counter, pid \\ :self) do
    pid = resolve_pid(pid)
    Agent.update(__MODULE__, fn state -> Map.put(state, pid, counter) end)
  end

  def inc(pid \\ :self) do
    pid = resolve_pid(pid)
    Agent.update(__MODULE__, fn state -> Map.put(state, pid, Map.get(state, pid, 0) + 1) end)
  end

  def dec(pid \\ :self) do
    pid = resolve_pid(pid)
    Agent.update(__MODULE__, fn state -> Map.put(state, pid, Map.get(state, pid, 0) - 1) end)
  end

  def get(default, pid \\ :self) do
    pid = resolve_pid(pid)
    Agent.get_and_update(__MODULE__, fn state ->
      Map.get_and_update(state, pid, fn
        nil ->
          {default, default}

        current_value ->
          {current_value, current_value}
      end)
    end)
  end

  defp resolve_pid(:self), do: self()
  defp resolve_pid(pid), do: pid
end
