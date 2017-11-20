defmodule Sage.Adapters.DefensiveRecursion do
  @moduledoc """
  This module is responsible for Sage execution implementation.
  """
  @behaviour Sage.Adapter
  require Logger

  # # Inline functions for performance optimization
  # @compile {:inline, encode_integer: 1, encode_float: 1}

  @impl true
  def execute(%Sage{} = sage, opts) do
    %{operations: operations, finally: finally, on_compensation_error: on_compensation_error, tracers: tracers} = sage
    inital_state = {nil, %{}, {1, []}, false, [], on_compensation_error, tracers}
    final_hooks = MapSet.to_list(finally)

    operations
    |> Enum.reverse()
    |> execute_transactions([], opts, inital_state)
    |> maybe_notify_final_hooks(final_hooks, opts)
    |> return_or_reraise()
  end

  # Transactions

  defp execute_transactions([], executed_operations, opts, state) do
    {last_effect, effects_so_far, _retries, _abort?, tasks, _on_compensation_error, _tracers} = state

    if tasks == [] do
      {:ok, last_effect, effects_so_far}
    else
      {:next_transaction, state}
      |> maybe_await_for_tasks(tasks)
      |> handle_transaction_result()
      |> execute_next_operation([], executed_operations, opts) # TODO: Do actual return in execute_next_operation?
    end
  end

  defp execute_transactions([{name, operation} | operations], executed_operations, opts, state) do
    {_last_effect_or_error, _effects_so_far, _retries, _abort?, tasks, _on_compensation_error, _tracers} = state

    {{name, operation}, state} # TODO: Remove second tuple here
    # |> maybe_notify_tracers()
    |> maybe_await_for_tasks(tasks)
    |> maybe_execute_transaction(opts)
    |> handle_transaction_result()
    # |> maybe_notify_tracers()
    |> execute_next_operation(operations, executed_operations, opts)
  end

  defp maybe_await_for_tasks({operation, state}, []), do: {operation, state}

  # If next operation is async, we don't need to await for tasks
  defp maybe_await_for_tasks({{_name, {:run_async, _transaction, _compensation, _opts}} = operation, state}, _tasks), do: {operation, state}

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

      {:exit, {{:nocatch, reason}, _stacktract}} ->
        {name, {:throw, reason}}

      {:exit, {exception, stacktrace}} ->
        {name, {:raise, {exception, stacktrace}}}

      {:exit, reason} ->
        {name, {:exit, reason}}

      nil ->
        {name, {:raise, {%Sage.AsyncTransactionTimeoutError{name: name, timeout: timeout}, System.stacktrace()}}}
    end
  end

  defp handle_task_result({name, result}, {:start_compensations, state}) do
    failure_reason = elem(state, 0)
    case handle_transaction_result({name, :async, result, state}) do
      {:next_transaction, {^name, :async}, state} ->
        {:start_compensations, put_elem(state, 0, failure_reason)}

      {:start_compensations, {^name, :async}, state} ->
        {:start_compensations, put_elem(state, 0, failure_reason)}
    end
  end

  defp handle_task_result({name, result}, {operation_or_command, state}) do
    case handle_transaction_result({name, :async, result, state}) do
      {:next_transaction, {^name, :async}, state} ->
        {operation_or_command, state}

      {:start_compensations, {^name, :async}, state} ->
        {:start_compensations, state}
    end
  end

  defp maybe_execute_transaction({:start_compensations, state}, _opts), do: {:start_compensations, state}

  defp maybe_execute_transaction({{name, operation}, state}, opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, _tasks, _on_compensation_error, _tracers} = state
    {name, operation, execute_transaction(operation, effects_so_far, opts), state}
  end

  defp execute_transaction({:run, transaction, _compensation, []}, effects_so_far, opts) do
    apply_transaction_fun(transaction, effects_so_far, opts)
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
    :throw, reason -> {:throw, reason}
  end

  defp execute_transaction({:run_async, transaction, _compensation, tx_opts}, effects_so_far, opts) do
    task =
      Task.Supervisor.async_nolink(Sage.AsyncTransactionSupervisor, fn ->
        apply_transaction_fun(transaction, effects_so_far, opts)
      end)

    {task, tx_opts}
  end

  defp apply_transaction_fun({mod, fun, args} = mfa, effects_so_far, opts) do
    apply(mod, fun, [effects_so_far, opts | args])
  else
    {:ok, effect} ->
      {:ok, effect}

    {:error, reason} ->
      {:error, reason}

    {:abort, reason} ->
      {:abort, reason}

    other ->
      {:raise, {%Sage.MalformedTransactionReturnError{transaction: mfa, return: other}, System.stacktrace()}}
  end

  defp apply_transaction_fun(fun, effects_so_far, opts) do
    apply(fun, [effects_so_far, opts])
  else
    {:ok, effect} ->
      {:ok, effect}

    {:error, reason} ->
      {:error, reason}

    {:abort, reason} ->
      {:abort, reason}

    other ->
      {:raise, {%Sage.MalformedTransactionReturnError{transaction: fun, return: other}, System.stacktrace()}}
  end

  defp handle_transaction_result({:start_compensations, state}), do: {:start_compensations, state}

  defp handle_transaction_result({:next_transaction, state}), do: {:next_transaction, state}

  defp handle_transaction_result({name, operation, {%Task{}, _async_opts} = async_job, state}) do
    {last_effect_or_error, effects_so_far, retries, _abort?, tasks, on_compensation_error, tracers} = state
    state = {last_effect_or_error, effects_so_far, retries, false, [{name, async_job} | tasks], on_compensation_error, tracers}
    {:next_transaction, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:ok, effect}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {effect, Map.put(effects_so_far, name, effect), retries, false, [], on_compensation_error, tracers}
    {:next_transaction, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:abort, reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {{name, reason}, Map.put(effects_so_far, name, reason), retries, true, [], on_compensation_error, tracers}
    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:error, reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, [], on_compensation_error, tracers} = state
    state = {{name, reason}, Map.put(effects_so_far, name, reason), retries, false, [], on_compensation_error, tracers}
    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:raise, reraise_args}, state}) do
    {_last_effect_or_error, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    state = {{:raise, reraise_args}, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:throw, reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    state = {{:throw, reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:exit, exit_reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    state = {{:exit, exit_reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {:start_compensations, {name, operation}, state}
  end

  # Compensation

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
      {:throw, reason} -> {:throw, reason}
      {_name, reason} -> {:error, reason}
    end
  end

  defp execute_compensation({{name, {:run, _operation, :noop, _tx_opts} = operation}, state}, _opts) do
    {name, operation, :ok, nil, state}
  end

  defp execute_compensation({{name, {_type, _operation, compensation, _tx_opts} = operation}, state}, opts) do
    {{failed_name, failed_reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    {effect_to_compensate, effects_so_far} = Map.pop(effects_so_far, name)
    return = safe_apply_compensation_fun(compensation, effect_to_compensate, {failed_name, failed_reason}, opts)
    state = {{failed_name, failed_reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {name, operation, return, effect_to_compensate, state}
  end

  defp safe_apply_compensation_fun(compensation, effect_to_compensate, name_and_reason, opts) do
    apply_compensation_fun(compensation, effect_to_compensate, name_and_reason, opts)
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
    :throw, error -> {:throw, error}
  else
    :ok -> :ok
    :abort -> :abort
    {:retry, retry_opts} -> {:retry, retry_opts}
    {:continue, effect} -> {:continue, effect}
    other -> {:raise, {%Sage.MalformedCompensationReturnError{compensation: compensation, return: other}, System.stacktrace()}}
  end

  defp apply_compensation_fun({mod, fun, args}, effect_to_compensate, {name, reason}, opts),
    do: apply(mod, fun, [effect_to_compensate, {name, reason}, opts | args])
  defp apply_compensation_fun(fun, effect_to_compensate, {name, reason}, opts),
    do: apply(fun, [effect_to_compensate, {name, reason}, opts])

  defp handle_compensation_result({name, operation, :ok, _compensated_effect, state}) do
    {:next_compensation, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, :abort, _compensated_effect, state}) do
    state = put_elem(state, 3, true)
    {:next_compensation, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, {:retry, _retry_opts}, _compensated_effect, {_last_error, _effects_so_far, _retries, true, [], _on_compensation_error, _tracers} = state}) do
    {:next_compensation, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, {:retry, retry_opts}, _compensated_effect, {last_effect_or_error, effects_so_far, {count, _old_retry_opts}, false, [], on_compensation_error, tracers} = state}) do
    if Keyword.fetch!(retry_opts, :retry_limit) > count do
      state = {last_effect_or_error, effects_so_far, {count + 1, retry_opts}, false, [], on_compensation_error, tracers}
      {:retry_transaction, {name, operation}, state}
    else
      {:next_compensation, {name, operation}, state}
    end
  end
  defp handle_compensation_result({name, operation, {:continue, effect}, _compensated_effect, {{name, _return_reason}, effects_so_far, retries, false, [], on_compensation_error, tracers}}) do
    state = {effect, Map.put(effects_so_far, name, effect), retries, false, [], on_compensation_error, tracers}
    {:next_transaction, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, {:continue, _effect}, _compensated_effect, state}) do
    {{return_name, _return_reason}, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    exception = %Sage.UnexpectedCircuitBreakError{compensation_name: name, failed_transaction_name: return_name}
    return = {:raise, {exception, System.stacktrace()}}
    state = {return, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {:next_compensation, {name, operation}, state}
  end
  defp handle_compensation_result({name, operation, {:raise, _} = to_raise, compensated_effect, state}) do
    state = put_elem(state, 0, to_raise)
    {:compensation_error_handler, {name, operation, compensated_effect}, state}
  end
  defp handle_compensation_result({name, operation, {:exit, _reason} = error, compensated_effect, state}) do
    state = put_elem(state, 0, error)
    {:compensation_error_handler, {name, operation, compensated_effect}, state}
  end
  defp handle_compensation_result({name, operation, {:throw, _error} = error, compensated_effect, state}) do
    state = put_elem(state, 0, error)
    {:compensation_error_handler, {name, operation, compensated_effect}, state}
  end

  # Shared

  defp execute_next_operation({:next_transaction, {name, operation}, state}, operations, executed_operations, opts) do
    execute_transactions(operations, [{name, operation} | executed_operations], opts, state)
  end

  defp execute_next_operation({:next_transaction, state}, [], [{prev_name, _prev_operation} | executed_operations], opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, [], _on_compensation_error, _tracers} = state
    state = put_elem(state, 0, Map.get(effects_so_far, prev_name))
    execute_transactions([], executed_operations, opts, state)
  end

  defp execute_next_operation({:start_compensations, {name, operation}, state}, compensated_operations, operations, opts) do
    execute_compensations(compensated_operations, [{name, operation} | operations], opts, state)
  end

  defp execute_next_operation({:start_compensations, state}, compensated_operations, operations, opts) do
    execute_compensations(compensated_operations, operations, opts, state)
  end

  defp execute_next_operation({:next_compensation, {name, operation}, state}, compensated_operations, operations, opts) do
    execute_compensations([{name, operation} | compensated_operations], operations, opts, state)
  end

  defp execute_next_operation({:retry_transaction, {name, operation}, state}, compensated_operations, operations, opts) do
    execute_transactions([{name, operation} | compensated_operations], operations, opts, state)
  end

  defp execute_next_operation({:compensation_error_handler, {name, operation, compensated_effect}, state}, _compensated_operations, operations, opts) do
    {error, effects_so_far, _retries, _abort?, [], on_compensation_error, _tracers} = state
    if on_compensation_error == :raise do
      return_or_reraise(error)
    else
      compensations_to_run =
        [{name, operation} | operations]
        |> Enum.reduce([], fn
          {_name, {_type, _operation, :noop, _tx_opts}}, acc ->
            acc

          {^name, {_type, _operation, compensation, _tx_opts}}, acc ->
            acc ++ [{name, compensation, compensated_effect}]

          {name, {_type, _operation, compensation, _tx_opts}}, acc ->
            acc ++ [{name, compensation, Map.fetch!(effects_so_far, name)}]
        end)

      # Logger.warn("Using compensation error handler because #{name} had an error #{inspect(error)}")

      case error do
        {:raise, {exception, stacktrace}} -> apply(on_compensation_error, :handle_error, [{:exception, exception, stacktrace}, compensations_to_run, opts])
        {:exit, reason} -> apply(on_compensation_error, :handle_error, [{:exit, reason}, compensations_to_run, opts])
        {:throw, error} -> apply(on_compensation_error, :handle_error, [{:throw, error}, compensations_to_run, opts])
      end
    end
  end

  # defp log

  defp maybe_notify_final_hooks(result, [], _opts), do: result

  defp maybe_notify_final_hooks(result, filanlize_callbacks, opts) do
    status = if elem(result, 0) == :ok, do: :ok, else: :error
    # credo:disable-for-lines:11
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

  defp apply_and_resque_errors(module, function, arguments) do
    apply(module, function, arguments)
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
    :throw, reason -> {:throw, reason}
  end

  defp apply_and_resque_errors(function, arguments) do
    apply(function, arguments)
  # TODO: This block can be replaced with catch :error, error
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
    :throw, reason -> {:throw, reason}
  end

  defp maybe_log_errors({from, {:raise, {exception, stacktrace}}}) do
    Logger.error("""
    [Sage] final hook exception from #{callback_to_string(from)} is ignored:

      #{Exception.format(:error, exception, stacktrace)}
    """)
  end
  defp maybe_log_errors({from, {:throw, reason}}) do
    Logger.error("[Sage] final hook error from #{callback_to_string(from)} is ignored. Error: #{inspect(reason)}")
  end
  defp maybe_log_errors({from, {:exit, reason}}) do
    Logger.error("[Sage] final hook exit from #{callback_to_string(from)} is ignored. Exit reason: #{inspect(reason)}")
  end
  defp maybe_log_errors({_from, _other}) do
    :ok
  end

  defp callback_to_string({m, f, a}), do: "#{to_string(m)}.#{to_string(f)}/#{to_string(length(a))}"
  defp callback_to_string({f, _a}), do: inspect(f)

  defp return_or_reraise({:ok, effect, other_effects}), do: {:ok, effect, other_effects}
  defp return_or_reraise({:exit, reason}), do: exit(reason)
  defp return_or_reraise({:throw, reason}), do: throw(reason)
  defp return_or_reraise({:raise, {exception, stacktrace}}), do: reraise(exception, stacktrace)
  defp return_or_reraise({:error, reason}), do: {:error, reason}
end
