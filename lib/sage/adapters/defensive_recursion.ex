defmodule Sage.Adapters.DefensiveRecursion do
  @moduledoc """
  This module is responsible for Sage execution implementation.
  """
  @behaviour Sage.Adapter
  require Logger

  # # Inline functions for performance optimization
  # @compile {:inline, encode_integer: 1, encode_float: 1}

  @doc false
  @impl true
  def execute(%Sage{} = sage, opts) do
    %{operations: operations, finally: finally, on_compensation_error: on_compensation_error, tracers: tracers} = sage
    inital_state = {nil, %{}, {0, []}, false, [], on_compensation_error, tracers}

    operations
    |> Enum.reverse()
    |> execute_transactions([], opts, inital_state)
    |> maybe_call_final_hooks(finally, opts)
    |> return_or_raise()
  end

  defp maybe_call_final_hooks(result, [], _opts), do: result

  defp maybe_call_final_hooks(result, filanlize_callbacks, opts) do
    status = if elem(result, 0) == :ok, do: :ok, else: :error
    # TODO: What if finalize raises an error?
    # credo:disable-for-lines:6
    filanlize_callbacks
    |> Enum.map(fn
      {module, function, args} ->
        args = [status, opts | args]
        {{module, function, args}, apply_and_resque_errors(module, function, args)}
      callback ->
        args = [status, opts]
        {{callback, args}, apply_and_resque_errors(callback, args)}
    end)
    |> Enum.map(&maybe_log_errors/1)

    result
  end

  defp maybe_log_errors({from, {:raise, {exception, stacktrace}}}) do
    Logger.error("""
    [Sage] final hook exception from #{callback_to_string(from)} is ignored:

      #{Exception.format(:error, exception, stacktrace)}
    """)
  end
  defp maybe_log_errors({from, {:exit, reason}}) do
    Logger.error("[Sage] final hook exit from #{callback_to_string(from)} is ignored. Exit reason: #{inspect(reason)}")
  end
  defp maybe_log_errors({_from, _other}), do: :ok

  defp callback_to_string({m, f, a}), do: "#{to_string(m)}.#{to_string(f)}/#{to_string(length(a))}"
  defp callback_to_string({f, _a}), do: inspect(f)

  defp return_or_raise({:ok, effect, other_effects}), do: {:ok, effect, other_effects}
  defp return_or_raise({:exit, reason}), do: exit(reason)
  defp return_or_raise({:raise, {exception, stacktrace}}), do: reraise(exception, stacktrace)
  defp return_or_raise({:error, reason}), do: {:error, reason}

  defp execute_transactions([], executed_operations, opts, state) do
    {last_effect, effects_so_far, _retries, _abort?, tasks, _on_compensation_error, _tracers} = state

    if tasks == [] do
      {:ok, last_effect, effects_so_far}
    else
      {:execute, state}
      |> maybe_await_for_tasks(tasks)
      |> handle_operation_result()
      |> execute_next_operation([], executed_operations, opts) # TODO: Do actual return in execute_next_operation?
    end
  end

  defp execute_transactions([{name, operation} | operations], executed_operations, opts, state) do
    {_last_effect_or_error, _effects_so_far, _retries, _abort?, tasks, _on_compensation_error, _tracers} = state

    {{name, operation}, state} # TODO: Remove second tuple here
    # |> maybe_notify_tracers()
    |> maybe_await_for_tasks(tasks)
    |> maybe_execute_transaction(opts)
    |> handle_operation_result()
    # |> maybe_notify_tracers()
    |> execute_next_operation(operations, executed_operations, opts)
  end

  defp maybe_await_for_tasks({operation, state}, []), do: {operation, state}

  defp maybe_await_for_tasks({operation, state}, tasks) do
    state = put_elem(state, 4, [])

    tasks
    |> Enum.map(&await_for_task/1)
    |> Enum.reduce({operation, state}, &handle_task_result/2)
  end

  defp await_for_task({name, {task, yield_opts}}) do
    timeout = Keyword.get(yield_opts, :timeout, 5000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, result} ->
        {name, result}

      {:exit, {exception, stacktrace}} ->
        {name, {:raise, {exception, stacktrace}}}

      {:exit, reason} ->
        {name, {:exit, reason}}

      nil ->
        {name, {:raise, {%Sage.AsyncTransactionTimeoutError{name: name, timeout: timeout}, System.stacktrace()}}}
    end
  end

  defp handle_task_result({name, result}, {operation_or_command, state}) do
    case handle_operation_result({name, :async, result, state}) do
      {:execute, {^name, :async}, state} ->
        {operation_or_command, state}

      {:compensate, {^name, :async}, state} ->
        {:compensate, state}
    end
  end

  defp maybe_execute_transaction({:compensate, state}, _opts), do: {:compensate, state}

  defp maybe_execute_transaction({{name, operation}, state}, opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, _tasks, _on_compensation_error, _tracers} = state
    {name, operation, execute_transaction(operation, effects_so_far, opts), state}
  end

  defp execute_transaction({:run, transaction, _compensation, []}, effects_so_far, opts) do
    try do
      transaction
      |> apply_transaction_fun(effects_so_far, opts)
      |> validate_transaction_return!(transaction)
    rescue
      exception ->
        {:raise, {exception, System.stacktrace()}}
    catch
      :exit, reason ->
        {:exit, reason}
    end
  end

  defp execute_transaction({:run_async, transaction, _compensation, tx_opts}, effects_so_far, opts) do
    task =
      Task.Supervisor.async_nolink(Sage.AsyncTransactionSupervisor, fn ->
        transaction
        |> apply_transaction_fun(effects_so_far, opts)
        |> validate_transaction_return!(transaction)
      end)

    {task, tx_opts}
  end

  defp validate_transaction_return!({:ok, effect}, _transaction), do: {:ok, effect}
  defp validate_transaction_return!({:error, reason}, _transaction), do: {:error, reason}
  defp validate_transaction_return!({:abort, reason}, _transaction), do: {:abort, reason}

  defp validate_transaction_return!(other, transaction) do
    {:raise, {%Sage.MalformedTransactionReturnError{transaction: transaction, return: other}, System.stacktrace()}}
  end

  defp handle_operation_result({:compensate, state}), do: {:compensate, state}
  defp handle_operation_result({:execute, state}), do: {:execute, state}

  defp handle_operation_result({name, operation, {%Task{}, _async_opts} = async_job, state}) do
    {last_effect_or_error, effects_so_far, retries, _abort?, tasks, on_compensation_error, tracers} = state
    state = {last_effect_or_error, effects_so_far, retries, false, [{name, async_job} | tasks], on_compensation_error, tracers}
    {:execute, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:ok, effect}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {effect, Map.put(effects_so_far, name, effect), retries, false, [], on_compensation_error, tracers}
    {:execute, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:abort, reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {{name, reason}, Map.put(effects_so_far, name, reason), retries, true, [], on_compensation_error, tracers}
    {:compensate, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:error, reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {{name, reason}, Map.put(effects_so_far, name, reason), retries, false, [], on_compensation_error, tracers}
    {:compensate, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:raise, reraise_args}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {{:raise, reraise_args}, effects_so_far, retries, true, [], on_compensation_error, tracers}
    {:compensate, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:exit, exit_reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {{:exit, exit_reason}, effects_so_far, retries, true, [], on_compensation_error, tracers}
    {:compensate, {name, operation}, state}
  end

  defp execute_compensations(compensated_operations, [{name, operation} | operations], opts, state) do
    {{name, operation}, state}
    # |> maybe_notify_tracers()
    |> execute_compensation(opts)
    |> handle_compensation_result()
    # |> maybe_notify_tracers()
    |> execute_next_operation(compensated_operations, operations, opts)
  end

  defp execute_compensations(_compensated_operations, [], _opts, state) do
    {last_error, %{}, _retries, _abort?, [], _on_compensation_error, _tracers} = state

    case last_error do
      {:exit, reason} -> {:exit, reason}
      {:raise, {exception, stacktrace}} -> {:raise, {exception, stacktrace}}
      {_name, reason} -> {:error, reason}
    end
  end

  defp handle_compensation_result({name, operation, :ok, state}) do
    {:continue_compensate, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, :abort, state}) do
    state = put_elem(state, 3, true)
    {:continue_compensate, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, {:retry, _retry_opts}, {_last_error, _effects_so_far, _retries, true, [], _on_compensation_error, _tracers} = state}) do
    {:continue_compensate, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, {:retry, retry_opts}, {last_effect_or_error, effects_so_far, {count, _old_retry_opts}, false, [], on_compensation_error, tracers} = state}) do
    if Keyword.fetch!(retry_opts, :retry_limit) > count do
      state = {last_effect_or_error, effects_so_far, {count + 1, retry_opts}, false, [], on_compensation_error, tracers}
      {:now_execute, {name, operation}, state}
    else
      {:continue_compensate, {name, operation}, state}
    end
  end
  defp handle_compensation_result({name, operation, {:continue, effect}, {{name, _return_reason}, effects_so_far, retries, false, [], on_compensation_error, tracers}}) do
    state = {effect, Map.put(effects_so_far, name, effect), retries, false, [], on_compensation_error, tracers}
    {:execute, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, {:continue, _effect}, state}) do
    {{return_name, _return_reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    exception = {:raise, {%Sage.UnexpectedCircuitBreakError{compensation_name: name, failed_transaction_name: return_name}, System.stacktrace()}}
    state = {exception, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {:continue_compensate, {name, operation}, state}
  end

  defp execute_next_operation({:execute, {name, operation}, state}, operations, executed_operations, opts) do
    execute_transactions(operations, [{name, operation} | executed_operations], opts, state)
  end

  defp execute_next_operation({:execute, state}, [], [{prev_name, _prev_operation} | executed_operations], opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, [], _on_compensation_error, _tracers} = state
    state = put_elem(state, 0, Map.get(effects_so_far, prev_name))
    execute_transactions([], executed_operations, opts, state)
  end

  defp execute_next_operation({:compensate, {name, operation}, state}, compensated_operations, operations, opts) do
    execute_compensations(compensated_operations, [{name, operation} | operations], opts, state)
  end

  defp execute_next_operation({:compensate, state}, compensated_operations, operations, opts) do
    execute_compensations(compensated_operations, operations, opts, state)
  end

  defp execute_next_operation({:continue_compensate, {name, operation}, state}, compensated_operations, operations, opts) do
    execute_compensations([{name, operation} | compensated_operations], operations, opts, state)
  end

  defp execute_next_operation({:now_execute, {name, operation}, state}, compensated_operations, operations, opts) do
    execute_transactions([{name, operation} | compensated_operations], operations, opts, state)
  end

  defp execute_compensation({{name, {:run, _operation, :noop, _tx_opts} = operation, state}}, _opts) do
    {name, operation, :ok, state}
  end

  defp execute_compensation({{name, {_type, _operation, compensation, _tx_opts} = operation}, state}, opts) do
    {{failed_name, failed_reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    {effect_to_compensate, effects_so_far} = Map.pop(effects_so_far, name)
    state = {{failed_name, failed_reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {name, operation, apply_compensation_fun(compensation, effect_to_compensate, {failed_name, failed_reason}, opts), state}
  end

  defp apply_transaction_fun({mod, fun, args}, effects_so_far, opts), do: apply(mod, fun, [effects_so_far, opts | args])
  defp apply_transaction_fun(fun, effects_so_far, opts), do: apply(fun, [effects_so_far, opts])

  defp apply_compensation_fun({mod, fun, args}, effect_to_compensate, {name, reason}, opts),
    do: apply(mod, fun, [effect_to_compensate, {name, reason}, opts | args])

  defp apply_compensation_fun(fun, effect_to_compensate, {name, reason}, opts),
    do: apply(fun, [effect_to_compensate, {name, reason}, opts])

  defp apply_and_resque_errors(module, function, arguments) do
    apply(module, function, arguments)
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
  end

  defp apply_and_resque_errors(function, arguments) do
    apply(function, arguments)
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
  end
end
