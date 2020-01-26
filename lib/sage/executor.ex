defmodule Sage.Executor do
  @moduledoc """
  This module is responsible for Sage execution.
  """
  require Logger
  alias Sage.Executor.Retries

  # List of Sage.Executor functions we do not want to purge from stacktrace
  @stacktrace_functions_whitelist [:execute]

  # # Inline functions for performance optimization
  # @compile {:inline, encode_integer: 1, encode_float: 1}

  @spec execute(sage :: Sage.t(), opts :: any()) :: {:ok, result :: any(), effects :: Sage.effects()} | {:error, any()}
  def execute(sage, opts \\ [])

  def execute(%Sage{stages: []}, _opts), do: raise(Sage.EmptyError)

  def execute(%Sage{} = sage, opts) do
    %{
      stages: stages,
      final_hooks: final_hooks,
      on_compensation_error: on_compensation_error,
      tracers: tracers
    } = sage

    inital_state = {nil, %{}, {1, []}, false, [], on_compensation_error, {MapSet.to_list(tracers), opts}}
    final_hooks = MapSet.to_list(final_hooks)

    stages
    |> Enum.reverse()
    |> execute_transactions([], opts, inital_state)
    |> maybe_notify_final_hooks(final_hooks, opts)
    |> return_or_reraise()
  end

  # Transactions

  defp execute_transactions([], executed_stages, opts, state) do
    {last_effect, effects_so_far, _retries, _abort?, tasks, _on_compensation_error, _tracers} = state

    if tasks == [] do
      {:ok, last_effect, effects_so_far}
    else
      {:next_transaction, state}
      |> maybe_await_for_tasks(tasks)
      |> handle_transaction_result()
      |> execute_next_stage([], executed_stages, opts)
    end
  end

  defp execute_transactions([stage | stages], executed_stages, opts, state) do
    {_last_effect_or_error, _effects_so_far, _retries, _abort?, tasks, _on_compensation_error, _tracers} = state

    {stage, state}
    |> maybe_await_for_tasks(tasks)
    |> maybe_execute_transaction(opts)
    |> handle_transaction_result()
    |> execute_next_stage(stages, executed_stages, opts)
  end

  defp maybe_await_for_tasks({stage, state}, []), do: {stage, state}

  # If next stage has async transaction, we don't need to await for tasks
  defp maybe_await_for_tasks({{_name, {:run_async, _transaction, _compensation, _opts}} = stage, state}, _tasks),
    do: {stage, state}

  defp maybe_await_for_tasks({stage, state}, tasks) do
    state = put_elem(state, 4, [])

    tasks
    |> Enum.map(&await_for_task/1)
    |> Enum.reduce({stage, state}, &handle_task_result/2)
  end

  defp await_for_task({name, {task, yield_opts}}) do
    timeout = Keyword.get(yield_opts, :timeout, 5000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, result} ->
        {name, result}

      {:exit, {{:nocatch, reason}, _stacktrace}} ->
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
    state = put_elem(state, 6, maybe_notify_tracers(elem(state, 6), :finish_transaction, name))

    case handle_transaction_result({name, :async, result, state}) do
      {:next_transaction, {^name, :async}, state} ->
        {:start_compensations, put_elem(state, 0, failure_reason)}

      {:start_compensations, {^name, :async}, state} ->
        {:start_compensations, put_elem(state, 0, failure_reason)}
    end
  end

  defp handle_task_result({name, result}, {stage, state}) do
    state = put_elem(state, 6, maybe_notify_tracers(elem(state, 6), :finish_transaction, name))

    case handle_transaction_result({name, :async, result, state}) do
      {:next_transaction, {^name, :async}, state} ->
        {stage, state}

      {:start_compensations, {^name, :async}, state} ->
        {:start_compensations, state}
    end
  end

  defp maybe_execute_transaction({:start_compensations, state}, _opts), do: {:start_compensations, state}

  defp maybe_execute_transaction({{name, operation}, state}, opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, _tasks, _on_compensation_error, tracers} = state
    tracers = maybe_notify_tracers(tracers, :start_transaction, name)
    return = execute_transaction(operation, name, effects_so_far, opts)

    tracers =
      case return do
        {%Task{}, _async_opts} -> tracers
        _other -> maybe_notify_tracers(tracers, :finish_transaction, name)
      end

    state = put_elem(state, 6, tracers)
    {name, operation, return, state}
  end

  defp execute_transaction({:run, transaction, _compensation, []}, name, effects_so_far, opts) do
    apply_transaction_fun(name, transaction, effects_so_far, opts)
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
    :throw, reason -> {:throw, reason}
  end

  defp execute_transaction({:run_async, transaction, _compensation, tx_opts}, name, effects_so_far, opts) do
    logger_metadata = Logger.metadata()

    task =
      Task.Supervisor.async_nolink(Sage.AsyncTransactionSupervisor, fn ->
        Logger.metadata(logger_metadata)
        apply_transaction_fun(name, transaction, effects_so_far, opts)
      end)

    {task, tx_opts}
  end

  defp apply_transaction_fun(name, {mod, fun, args} = mfa, effects_so_far, opts) do
    case apply(mod, fun, [effects_so_far, opts | args]) do
      {:ok, effect} ->
        {:ok, effect}

      {:error, reason} ->
        {:error, reason}

      {:abort, reason} ->
        {:abort, reason}

      other ->
        {:raise,
         {%Sage.MalformedTransactionReturnError{stage: name, transaction: mfa, return: other}, System.stacktrace()}}
    end
  end

  defp apply_transaction_fun(name, fun, effects_so_far, opts) do
    case apply(fun, [effects_so_far, opts]) do
      {:ok, effect} ->
        {:ok, effect}

      {:error, reason} ->
        {:error, reason}

      {:abort, reason} ->
        {:abort, reason}

      other ->
        {:raise,
         {%Sage.MalformedTransactionReturnError{stage: name, transaction: fun, return: other}, System.stacktrace()}}
    end
  end

  defp handle_transaction_result({:start_compensations, state}), do: {:start_compensations, state}

  defp handle_transaction_result({:next_transaction, state}), do: {:next_transaction, state}

  defp handle_transaction_result({name, operation, {%Task{}, _async_opts} = async_job, state}) do
    {last_effect_or_error, effects_so_far, retries, _abort?, tasks, on_compensation_error, tracers} = state
    tasks = [{name, async_job} | tasks]
    state = {last_effect_or_error, effects_so_far, retries, false, tasks, on_compensation_error, tracers}
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

  defp execute_compensations(compensated_stages, [stage | stages], opts, state) do
    {stage, state}
    |> execute_compensation(opts)
    |> handle_compensation_result()
    |> execute_next_stage(compensated_stages, stages, opts)
  end

  defp execute_compensations(_compensated_stages, [], _opts, state) do
    {last_error, %{}, _retries, _abort?, [], _on_compensation_error, _tracers} = state

    case last_error do
      {:exit, reason} -> {:exit, reason}
      {:raise, {exception, stacktrace}} -> {:raise, {exception, stacktrace}}
      {:throw, reason} -> {:throw, reason}
      {_name, reason} -> {:error, reason}
    end
  end

  defp execute_compensation({{name, {_type, _transaction, :noop, _tx_opts} = operation}, state}, _opts) do
    {name, operation, :ok, nil, state}
  end

  defp execute_compensation({{name, {_type, _transaction, compensation, _tx_opts} = operation}, state}, opts) do
    {name_and_reason, effects_so_far, retries, abort?, [], on_compensation_error, tracers} = state
    {effect_to_compensate, effects_so_far} = Map.pop(effects_so_far, name)
    tracers = maybe_notify_tracers(tracers, :start_compensation, name)
    return = safe_apply_compensation_fun(name, compensation, effect_to_compensate, effects_so_far, opts)
    tracers = maybe_notify_tracers(tracers, :finish_compensation, name)
    state = {name_and_reason, effects_so_far, retries, abort?, [], on_compensation_error, tracers}
    {name, operation, return, effect_to_compensate, state}
  end

  defp safe_apply_compensation_fun(name, compensation, effect_to_compensate, effects_so_far, opts) do
    apply_compensation_fun(compensation, effect_to_compensate, effects_so_far, opts)
  rescue
    exception -> {:raise, {exception, System.stacktrace()}}
  catch
    :exit, reason -> {:exit, reason}
    :throw, error -> {:throw, error}
  else
    :ok ->
      :ok

    :abort ->
      :abort

    {:retry, retry_opts} ->
      {:retry, retry_opts}

    {:continue, effect} ->
      {:continue, effect}

    other ->
      exception_struct = %Sage.MalformedCompensationReturnError{stage: name, compensation: compensation, return: other}
      {:raise, {exception_struct, System.stacktrace()}}
  end

  defp apply_compensation_fun({mod, fun, args}, effect_to_compensate, effects_so_far, opts),
    do: apply(mod, fun, [effect_to_compensate, effects_so_far, opts | args])

  defp apply_compensation_fun(fun, effect_to_compensate, effects_so_far, opts),
    do: apply(fun, [effect_to_compensate, effects_so_far, opts])

  defp handle_compensation_result({name, operation, :ok, _compensated_effect, state}) do
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, :abort, _compensated_effect, state}) do
    state = put_elem(state, 3, true)
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, {:retry, _retry_opts}, _compensated_effect, state})
       when elem(state, 3) do
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, {:retry, retry_opts}, _compensated_effect, state})
       when not elem(state, 3) do
    {last_effect_or_error, effects_so_far, {count, _old_retry_opts}, false, [], on_compensation_error, tracers} = state

    if Retries.retry_with_backoff?(count, retry_opts) do
      state = {last_effect_or_error, effects_so_far, {count + 1, retry_opts}, false, [], on_compensation_error, tracers}
      {:retry_transaction, {name, operation}, state}
    else
      {:next_compensation, {name, operation}, state}
    end
  end

  defp handle_compensation_result({name, operation, {:continue, effect}, _compensated_effect, state})
       when not elem(state, 3) and name == elem(elem(state, 0), 0) do
    {{name, _return_reason}, effects_so_far, retries, false, [], on_compensation_error, tracers} = state
    state = {effect, Map.put(effects_so_far, name, effect), retries, false, [], on_compensation_error, tracers}
    {:next_transaction, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, {:continue, _effect}, _compensated_effect, state}) do
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, {:raise, _} = to_raise, compensated_effect, state}) do
    state = put_elem(state, 0, to_raise)
    {:compensation_error, {name, operation, compensated_effect}, state}
  end

  defp handle_compensation_result({name, operation, {:exit, _reason} = error, compensated_effect, state}) do
    state = put_elem(state, 0, error)
    {:compensation_error, {name, operation, compensated_effect}, state}
  end

  defp handle_compensation_result({name, operation, {:throw, _error} = error, compensated_effect, state}) do
    state = put_elem(state, 0, error)
    {:compensation_error, {name, operation, compensated_effect}, state}
  end

  # Shared

  defp execute_next_stage({:next_transaction, {name, operation}, state}, stages, executed_stages, opts) do
    execute_transactions(stages, [{name, operation} | executed_stages], opts, state)
  end

  defp execute_next_stage({:next_transaction, state}, [], [{prev_name, _prev_op} | executed_stages], opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, [], _on_compensation_error, _tracers} = state
    state = put_elem(state, 0, Map.get(effects_so_far, prev_name))
    execute_transactions([], executed_stages, opts, state)
  end

  defp execute_next_stage({:start_compensations, {name, operation}, state}, compensated_stages, stages, opts) do
    execute_compensations(compensated_stages, [{name, operation} | stages], opts, state)
  end

  defp execute_next_stage({:start_compensations, state}, compensated_stages, stages, opts) do
    execute_compensations(compensated_stages, stages, opts, state)
  end

  defp execute_next_stage({:next_compensation, {name, operation}, state}, compensated_stages, stages, opts) do
    execute_compensations([{name, operation} | compensated_stages], stages, opts, state)
  end

  defp execute_next_stage({:retry_transaction, {name, operation}, state}, compensated_stages, stages, opts) do
    execute_transactions([{name, operation} | compensated_stages], stages, opts, state)
  end

  defp execute_next_stage({:compensation_error, compensation_error, state}, _compensated_stages, stages, opts) do
    {name, operation, compensated_effect} = compensation_error
    {error, effects_so_far, _retries, _abort?, [], on_compensation_error, _tracers} = state

    if on_compensation_error == :raise do
      return_or_reraise(error)
    else
      compensations_to_run =
        [{name, operation} | stages]
        |> Enum.reduce([], fn
          {_name, {_type, _transaction, :noop, _tx_opts}}, acc ->
            acc

          {^name, {_type, _transaction, compensation, _tx_opts}}, acc ->
            acc ++ [{name, compensation, compensated_effect}]

          {name, {_type, _transaction, compensation, _tx_opts}}, acc ->
            acc ++ [{name, compensation, Map.fetch!(effects_so_far, name)}]
        end)

      _ =
        Logger.warn("""
        [Sage] compensation #{inspect(name)} failed to compensate effect:

          #{inspect(compensated_effect)}

        #{compensation_error_message(error)}
        """)

      case error do
        {:raise, {exception, stacktrace}} ->
          apply(on_compensation_error, :handle_error, [{:exception, exception, stacktrace}, compensations_to_run, opts])

        {:exit, reason} ->
          apply(on_compensation_error, :handle_error, [{:exit, reason}, compensations_to_run, opts])

        {:throw, error} ->
          apply(on_compensation_error, :handle_error, [{:throw, error}, compensations_to_run, opts])
      end
    end
  end

  defp compensation_error_message({:raise, {exception, stacktrace}}) do
    """
    Because exception was raised:

      #{Exception.format(:error, exception, stacktrace)}

    """
  end

  defp compensation_error_message({:exit, reason}) do
    "Because of exit with reason: #{inspect(reason)}."
  end

  defp compensation_error_message({:throw, error}) do
    "Because of thrown error: #{inspect(error)}."
  end

  defp maybe_notify_tracers({[], _tracing_state} = tracers, _action, _name) do
    tracers
  end

  defp maybe_notify_tracers({tracers, tracing_state}, action, name) do
    tracing_state =
      Enum.reduce(tracers, tracing_state, fn tracer, tracing_state ->
        apply_and_catch_errors(tracer, :handle_event, [name, action, tracing_state])
      end)

    {tracers, tracing_state}
  end

  defp maybe_notify_final_hooks(result, [], _opts), do: result

  defp maybe_notify_final_hooks(result, filanlize_callbacks, opts) do
    status = if elem(result, 0) == :ok, do: :ok, else: :error

    :ok =
      filanlize_callbacks
      |> Enum.map(fn
        {module, function, args} ->
          args = [status, opts | args]
          {{module, function, args}, apply_and_catch_errors(module, function, args)}

        callback ->
          args = [status, opts]
          {{callback, args}, apply_and_catch_errors(callback, args)}
      end)
      |> Enum.each(&maybe_log_errors/1)

    result
  end

  defp apply_and_catch_errors(module, function, arguments) do
    apply(module, function, arguments)
  catch
    :error, exception -> {:raise, {exception, System.stacktrace()}}
    :exit, reason -> {:exit, reason}
    :throw, reason -> {:throw, reason}
  end

  defp apply_and_catch_errors(function, arguments) do
    apply(function, arguments)
  catch
    :error, exception -> {:raise, {exception, System.stacktrace()}}
    :exit, reason -> {:exit, reason}
    :throw, reason -> {:throw, reason}
  end

  defp maybe_log_errors({from, {:raise, {exception, stacktrace}}}) do
    Logger.error("""
    [Sage] Exception during #{callback_to_string(from)} final hook execution is ignored:

      #{Exception.format(:error, exception, stacktrace)}
    """)
  end

  defp maybe_log_errors({from, {:throw, reason}}) do
    Logger.error(
      "[Sage] Throw during #{callback_to_string(from)} final hook execution is ignored. " <> "Error: #{inspect(reason)}"
    )
  end

  defp maybe_log_errors({from, {:exit, reason}}) do
    Logger.error(
      "[Sage] Exit during #{callback_to_string(from)} final hook execution is ignored. " <>
        "Exit reason: #{inspect(reason)}"
    )
  end

  defp maybe_log_errors({_from, _other}) do
    :ok
  end

  defp callback_to_string({m, f, a}), do: "#{to_string(m)}.#{to_string(f)}/#{to_string(length(a))}"
  defp callback_to_string({f, _a}), do: inspect(f)

  defp return_or_reraise({:ok, effect, other_effects}), do: {:ok, effect, other_effects}
  defp return_or_reraise({:exit, reason}), do: exit(reason)
  defp return_or_reraise({:throw, reason}), do: throw(reason)
  defp return_or_reraise({:raise, {exception, stacktrace}}), do: filter_and_reraise(exception, stacktrace)
  defp return_or_reraise({:error, reason}), do: {:error, reason}

  defp filter_and_reraise(exception, stacktrace) do
    stacktrace =
      Enum.reject(stacktrace, &match?({__MODULE__, fun, _, _} when fun not in @stacktrace_functions_whitelist, &1))

    reraise exception, stacktrace
  end
end
