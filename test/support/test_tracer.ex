defmodule Sage.TestTracer do
  def handle_event(name, :start_transaction, tracing_state) do
    tracing_state =
      tracing_state
      |> Enum.into(%{})
      |> Map.put(:"tx_#{name}_started_at", System.monotonic_time())

    test_pid = Map.get(tracing_state, :test_pid)
    if test_pid, do: send(test_pid, {name, :start_transaction, tracing_state})
    tracing_state
  end

  def handle_event(name, :finish_transaction, tracing_state) do
    diff = System.monotonic_time() - Map.fetch!(tracing_state, :"tx_#{name}_started_at")

    test_pid = Map.get(tracing_state, :test_pid)
    if test_pid, do: send(test_pid, {name, :finish_transaction, diff, tracing_state})
    tracing_state
  end

  def handle_event(name, :start_compensation, tracing_state) do
    tracing_state =
      tracing_state
      |> Enum.into(%{})
      |> Map.put(:"cmp_#{name}_started_at", System.monotonic_time())

    test_pid = Map.get(tracing_state, :test_pid)
    if test_pid, do: send(test_pid, {name, :start_compensation, tracing_state})
    tracing_state
  end

  def handle_event(name, :finish_compensation, tracing_state) do
    diff = System.monotonic_time() - Map.fetch!(tracing_state, :"cmp_#{name}_started_at")

    test_pid = Map.get(tracing_state, :test_pid)
    if test_pid, do: send(test_pid, {name, :finish_compensation, diff, tracing_state})
    tracing_state
  end
end
