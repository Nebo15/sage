defmodule Sage.Executor do
  @moduledoc """
  This module is responsible for Sage execution implementation.
  """

  @doc false
  @spec execute(sage :: Sage.t(), opts :: any()) :: {:ok, result :: any(), effects :: Sage.effects()} | {:error, any()}
  def execute(%Sage{} = sage, opts) do
    sage.operations
    |> Enum.reverse()
    |> execute_transactions([], opts, {nil, %{}, {0, []}, false, []})
    |> filanlize(sage.finally, opts)
    |> return()
  end

  defp filanlize(result, [], _opts), do: result

  defp filanlize(result, filanlize_callbacks, opts) do
    status = if elem(result, 0) == :ok, do: :ok, else: :error
    # TODO: What if finalize raises an error?
    # credo:disable-for-lines:6
    Enum.map(filanlize_callbacks, fn
      {module, function, args} ->
        apply(module, function, [status, opts | args])

      callback ->
        apply(callback, [status, opts])
    end)

    result
  end

  defp return({:ok, effect, other_effects}), do: {:ok, effect, other_effects}
  defp return({:exit, reason}), do: exit(reason)
  defp return({:raise, {exception, stacktrace}}), do: reraise(exception, stacktrace)
  defp return({:error, reason}), do: {:error, reason}

  defp execute_transactions([{name, operation} | operations], executed_operations, opts, state) do
    {_last_effect_or_error, _effects_so_far, _retries, _abort?, tasks} = state

    {{name, operation}, state}
    |> maybe_await_for_tasks(tasks)
    |> maybe_execute_transaction(opts)
    |> handle_operation_result()
    |> execute_next_operation(operations, executed_operations, opts)
  end

  defp execute_transactions([], executed_operations, opts, state) do
    {last_effect, effects_so_far, _retries, _abort?, tasks} = state

    if tasks == [] do
      {:ok, last_effect, effects_so_far}
    else
      {:execute, state}
      |> maybe_await_for_tasks(tasks)
      |> handle_operation_result()
      |> execute_next_operation([], executed_operations, opts)
    end
  end

  defp maybe_await_for_tasks({operation, state}, []), do: {operation, state}

  defp maybe_await_for_tasks({operation, state}, tasks) do
    state = put_elem(state, 4, [])

    tasks
    |> Enum.map(fn {name, {task, yield_opts}} ->
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
       end)
    |> Enum.reduce({operation, state}, fn {name, result}, {operation, state} ->
         case handle_operation_result({name, :async, result, state}) do
           {:execute, {^name, :async}, state} ->
             {operation, state}

           {:compensate, {^name, :async}, state} ->
             {:compensate, state}
         end
       end)
  end

  defp maybe_execute_transaction({:compensate, state}, _opts), do: {:compensate, state}

  defp maybe_execute_transaction({{name, operation}, state}, opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, _tasks} = state
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
    {last_effect_or_error, effects_so_far, retries, _abort?, tasks} = state
    state = {last_effect_or_error, effects_so_far, retries, false, [{name, async_job} | tasks]}
    {:execute, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:ok, effect}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, []} = state
    state = {effect, Map.put(effects_so_far, name, effect), retries, false, []}
    {:execute, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:abort, reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, []} = state
    state = {{name, reason}, Map.put(effects_so_far, name, reason), retries, true, []}
    {:compensate, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:error, reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, []} = state
    state = {{name, reason}, Map.put(effects_so_far, name, reason), retries, false, []}
    {:compensate, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:raise, reraise_args}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, []} = state
    state = {{:raise, reraise_args}, effects_so_far, retries, true, []}
    {:compensate, {name, operation}, state}
  end

  defp handle_operation_result({name, operation, {:exit, exit_reason}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, []} = state
    state = {{:exit, exit_reason}, effects_so_far, retries, true, []}
    {:compensate, {name, operation}, state}
  end

  defp execute_next_operation({:execute, {name, operation}, state}, operations, executed_operations, opts) do
    execute_transactions(operations, [{name, operation} | executed_operations], opts, state)
  end

  defp execute_next_operation({:execute, state}, [], [{prev_name, _prev_operation} | executed_operations], opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, []} = state
    state = put_elem(state, 0, Map.get(effects_so_far, prev_name))
    execute_transactions([], executed_operations, opts, state)
  end

  defp execute_next_operation({:compensate, {name, operation}, state}, operations, executed_operations, opts) do
    execute_compensations([{name, operation} | executed_operations], operations, opts, state)
  end

  defp execute_next_operation({:compensate, state}, operations, executed_operations, opts) do
    execute_compensations(executed_operations, operations, opts, state)
  end

  defp execute_compensations([{name, operation} | operations], compensated_operations, opts, state) do
    {last_effect_or_error, effects_so_far, retries, abort?, []} = state
    {effect_to_compensate, effects_so_far} = Map.pop(effects_so_far, name)

    case execute_compensation(operation, effect_to_compensate, opts, state) do
      :ok ->
        state = {last_effect_or_error, effects_so_far, retries, abort?, []}
        execute_compensations(operations, [{name, operation} | compensated_operations], opts, state)

      :abort ->
        state = {last_effect_or_error, effects_so_far, retries, true, []}
        execute_compensations(operations, [{name, operation} | compensated_operations], opts, state)

      {:retry, retry_opts} ->
        {count, _retry_opts} = retries

        if abort? do
          state = {last_effect_or_error, effects_so_far, retries, abort?, []}
          execute_compensations(operations, [{name, operation} | compensated_operations], opts, state)
        else
          if Keyword.fetch!(retry_opts, :retry_limit) > count do
            state = {last_effect_or_error, effects_so_far, {count + 1, retry_opts}, false, []}
            execute_transactions([{name, operation} | compensated_operations], operations, opts, state)
          else
            state = {last_effect_or_error, effects_so_far, retries, abort?, []}
            execute_compensations(operations, [{name, operation} | compensated_operations], opts, state)
          end
        end

      {:continue, effect} ->
        {return_name, _return_reason} = last_effect_or_error

        if return_name == name do
          state = {effect, Map.put(effects_so_far, name, effect), retries, false, []}
          execute_transactions(compensated_operations, [{name, operation} | operations], opts, state)
        else
          raise Sage.UnexpectedCircuitBreakError, compensation_name: name, failed_transaction_name: return_name
        end
    end
  end

  defp execute_compensations([], _compensated_operations, _opts, state) do
    {last_error, %{}, _retries, _abort?, []} = state

    case last_error do
      {:exit, reason} -> {:exit, reason}
      {:raise, {exception, stacktrace}} -> {:raise, {exception, stacktrace}}
      {_name, reason} -> {:error, reason}
    end
  end

  defp execute_compensation({:run, _operation, :noop, _tx_opts}, _effect_to_compensate, _opts, _state), do: :ok

  defp execute_compensation({_type, _operation, compensation, _tx_opts}, effect_to_compensate, opts, state) do
    {{name, reason}, _effects_so_far, _retries, _abort?, _tasks} = state
    apply_compensation_fun(compensation, effect_to_compensate, {name, reason}, opts)
  end

  defp apply_transaction_fun({mod, fun, args}, effects_so_far, opts), do: apply(mod, fun, [effects_so_far, opts | args])
  defp apply_transaction_fun(fun, effects_so_far, opts), do: apply(fun, [effects_so_far, opts])

  defp apply_compensation_fun({mod, fun, args}, effect_to_compensate, {name, reason}, opts),
    do: apply(mod, fun, [effect_to_compensate, {name, reason}, opts | args])

  defp apply_compensation_fun(fun, effect_to_compensate, {name, reason}, opts),
    do: apply(fun, [effect_to_compensate, {name, reason}, opts])
end
