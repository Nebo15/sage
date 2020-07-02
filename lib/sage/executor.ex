defmodule Sage.Executor do
  @moduledoc """
  This module is responsible for Sage execution.
  """
  import Record
  alias Sage.Executor.Retries
  require Logger

  @type state ::
          record(:state,
            last_effect_or_error:
              {name :: term(), reason :: term()}
              | {:raise, exception :: Exception.t()}
              | {:throw, reason :: term()}
              | {:exit, reason :: term()},
            effects_so_far: map(),
            attempts: non_neg_integer(),
            aborted?: boolean(),
            tasks: [{Task.t(), Keyword.t()}],
            on_compensation_error: :raise | module(),
            tracers_and_state: {MapSet.t(), term()}
          )

  defrecordp(:state,
    last_effect_or_error: nil,
    effects_so_far: %{},
    attempts: 1,
    aborted?: false,
    tasks: [],
    on_compensation_error: :raise,
    tracers_and_state: {MapSet.new(), []}
  )

  # List of Sage.Executor functions we do not want to purge from stacktrace
  @stacktrace_functions_whitelist [:execute]

  # # TODO: Inline functions for performance optimization
  # @compile {:inline, encode_integer: 1, encode_float: 1}

  @spec execute(sage :: Sage.t(), attrs :: any()) :: {:ok, result :: any(), effects :: Sage.effects()} | {:error, any()}
  def execute(sage, attrs \\ [])

  def execute(%Sage{stages: []}, _opts), do: raise(Sage.EmptyError)

  def execute(%Sage{} = sage, attrs) do
    %{
      stages: stages,
      final_hooks: final_hooks,
      on_compensation_error: on_compensation_error,
      tracers: tracers
    } = sage

    inital_state =
      state(on_compensation_error: on_compensation_error, tracers_and_state: {MapSet.to_list(tracers), attrs})

    final_hooks = MapSet.to_list(final_hooks)

    stages
    |> Enum.reverse()
    |> execute_transactions([], attrs, inital_state)
    |> maybe_notify_final_hooks(final_hooks, attrs)
    |> return_or_reraise()
  end

  # Transactions

  defp execute_transactions([], _executed_stages, _opts, state(tasks: []) = state) do
    state(last_effect_or_error: last_effect, effects_so_far: effects_so_far) = state
    {:ok, last_effect, effects_so_far}
  end

  defp execute_transactions([], executed_stages, attrs, state) do
    state(tasks: tasks) = state

    {:next_transaction, state}
    |> maybe_await_for_tasks(tasks)
    |> handle_transaction_result()
    |> execute_next_stage([], executed_stages, attrs)
  end

  defp execute_transactions([stage | stages], executed_stages, attrs, state) do
    state(tasks: tasks) = state

    {stage, state}
    |> maybe_await_for_tasks(tasks)
    |> maybe_execute_transaction(attrs)
    |> handle_transaction_result()
    |> execute_next_stage(stages, executed_stages, attrs)
  end

  defp maybe_await_for_tasks({stage, state}, []), do: {stage, state}

  # If next stage has async transaction, we don't need to await for tasks
  defp maybe_await_for_tasks({{_name, {:run_async, _transaction, _compensation, _opts}} = stage, state}, _tasks),
    do: {stage, state}

  defp maybe_await_for_tasks({stage, state}, tasks) do
    state = state(state, tasks: [])

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
        {name, {:raise, %Sage.AsyncTransactionTimeoutError{name: name, timeout: timeout}}}
    end
  end

  defp handle_task_result({name, result}, {:start_compensations, state}) do
    state(last_effect_or_error: failure_reason, tracers_and_state: tracers_and_state) = state
    tracers_and_state = maybe_notify_tracers(tracers_and_state, :finish_transaction, name)
    state = state(state, tracers_and_state: tracers_and_state)

    case handle_transaction_result({name, :async, result, state}) do
      {:next_transaction, {^name, :async}, state} ->
        state = state(state, last_effect_or_error: failure_reason)
        {:start_compensations, state}

      {:start_compensations, {^name, :async}, state} ->
        state = state(state, last_effect_or_error: failure_reason)
        {:start_compensations, state}
    end
  end

  defp handle_task_result({name, result}, {stage, state}) do
    state(tracers_and_state: tracers_and_state) = state
    tracers_and_state = maybe_notify_tracers(tracers_and_state, :finish_transaction, name)
    state = state(state, tracers_and_state: tracers_and_state)

    case handle_transaction_result({name, :async, result, state}) do
      {:next_transaction, {^name, :async}, state} ->
        {stage, state}

      {:start_compensations, {^name, :async}, state} ->
        {:start_compensations, state}
    end
  end

  defp maybe_execute_transaction({:start_compensations, state}, _opts), do: {:start_compensations, state}

  defp maybe_execute_transaction({{name, operation}, state}, attrs) do
    state(effects_so_far: effects_so_far, tracers_and_state: tracers_and_state) = state
    tracers_and_state = maybe_notify_tracers(tracers_and_state, :start_transaction, name)

    return = execute_transaction(operation, name, effects_so_far, attrs)

    tracers_and_state =
      case return do
        {%Task{}, _async_opts} -> tracers_and_state
        _other -> maybe_notify_tracers(tracers_and_state, :finish_transaction, name)
      end

    state = state(state, tracers_and_state: tracers_and_state)
    {name, operation, return, state}
  end

  defp execute_transaction({:run, transaction, _compensation, []}, name, effects_so_far, attrs) do
    apply_transaction_fun(name, transaction, effects_so_far, attrs)
  rescue
    exception -> {:raise, {exception, __STACKTRACE__}}
  catch
    :exit, reason -> {:exit, reason}
    :throw, reason -> {:throw, reason}
  end

  defp execute_transaction({:run_async, transaction, _compensation, tx_opts}, name, effects_so_far, attrs) do
    logger_metadata = Logger.metadata()

    task =
      Task.Supervisor.async_nolink(Sage.AsyncTransactionSupervisor, fn ->
        _ = Logger.metadata(logger_metadata)
        apply_transaction_fun(name, transaction, effects_so_far, attrs)
      end)

    {task, tx_opts}
  end

  defp apply_transaction_fun(name, {mod, fun, args} = mfa, effects_so_far, attrs) do
    case apply(mod, fun, [effects_so_far, attrs | args]) do
      {:ok, effect} ->
        {:ok, effect}

      {:error, reason} ->
        {:error, reason}

      {:abort, reason} ->
        {:abort, reason}

      other ->
        {:raise, %Sage.MalformedTransactionReturnError{stage: name, transaction: mfa, return: other}}
    end
  end

  defp apply_transaction_fun(name, fun, effects_so_far, attrs) do
    case apply(fun, [effects_so_far, attrs]) do
      {:ok, effect} ->
        {:ok, effect}

      {:error, reason} ->
        {:error, reason}

      {:abort, reason} ->
        {:abort, reason}

      other ->
        {:raise, %Sage.MalformedTransactionReturnError{stage: name, transaction: fun, return: other}}
    end
  end

  defp handle_transaction_result({:start_compensations, state}), do: {:start_compensations, state}

  defp handle_transaction_result({:next_transaction, state}), do: {:next_transaction, state}

  defp handle_transaction_result({name, operation, {%Task{}, _async_opts} = async_step, state}) do
    state(tasks: tasks) = state
    state = state(state, tasks: [{name, async_step} | tasks], aborted?: false)
    {:next_transaction, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:ok, effect}, state}) do
    state(effects_so_far: effects_so_far) = state

    state =
      state(state, last_effect_or_error: effect, effects_so_far: Map.put(effects_so_far, name, effect), aborted?: false)

    {:next_transaction, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:abort, reason}, state}) do
    state(effects_so_far: effects_so_far) = state

    state =
      state(state,
        last_effect_or_error: {name, reason},
        effects_so_far: Map.put(effects_so_far, name, reason),
        aborted?: true
      )

    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:error, reason}, state}) do
    state(effects_so_far: effects_so_far) = state

    state =
      state(state,
        last_effect_or_error: {name, reason},
        effects_so_far: Map.put(effects_so_far, name, reason),
        aborted?: false
      )

    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:raise, exception}, state}) do
    state = state(state, last_effect_or_error: {:raise, exception})
    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:throw, reason}, state}) do
    state = state(state, last_effect_or_error: {:throw, reason})
    {:start_compensations, {name, operation}, state}
  end

  defp handle_transaction_result({name, operation, {:exit, reason}, state}) do
    state = state(state, last_effect_or_error: {:exit, reason})
    {:start_compensations, {name, operation}, state}
  end

  # Compensation

  defp execute_compensations(compensated_stages, [stage | stages], attrs, state) do
    {stage, state}
    |> execute_compensation(attrs)
    |> handle_compensation_result()
    |> execute_next_stage(compensated_stages, stages, attrs)
  end

  defp execute_compensations(_compensated_stages, [], _opts, state) do
    state(last_effect_or_error: last_error) = state

    case last_error do
      {:exit, reason} -> {:exit, reason}
      {:raise, {exception, stacktrace}} -> {:raise, {exception, stacktrace}}
      {:raise, exception} -> {:raise, exception}
      {:throw, reason} -> {:throw, reason}
      {_name, reason} -> {:error, reason}
    end
  end

  defp execute_compensation({{name, {_type, _transaction, :noop, _tx_opts} = operation}, state}, _opts) do
    {name, operation, :ok, nil, state}
  end

  defp execute_compensation({{name, {_type, _transaction, compensation, _tx_opts} = operation}, state}, attrs) do
    state(effects_so_far: effects_so_far, tracers_and_state: tracers_and_state) = state
    {effect_to_compensate, effects_so_far} = Map.pop(effects_so_far, name)
    tracers_and_state = maybe_notify_tracers(tracers_and_state, :start_compensation, name)
    return = safe_apply_compensation_fun(name, compensation, effect_to_compensate, effects_so_far, attrs)
    tracers_and_state = maybe_notify_tracers(tracers_and_state, :finish_compensation, name)
    state = state(state, effects_so_far: effects_so_far, tracers_and_state: tracers_and_state)
    {name, operation, return, effect_to_compensate, state}
  end

  defp safe_apply_compensation_fun(name, compensation, effect_to_compensate, effects_so_far, attrs) do
    apply_compensation_fun(compensation, effect_to_compensate, effects_so_far, attrs)
  rescue
    exception -> {:raise, {exception, __STACKTRACE__}}
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
      {:raise, %Sage.MalformedCompensationReturnError{stage: name, compensation: compensation, return: other}}
  end

  defp apply_compensation_fun({mod, fun, args}, effect_to_compensate, effects_so_far, attrs),
    do: apply(mod, fun, [effect_to_compensate, effects_so_far, attrs | args])

  defp apply_compensation_fun(fun, effect_to_compensate, effects_so_far, attrs),
    do: apply(fun, [effect_to_compensate, effects_so_far, attrs])

  defp handle_compensation_result({name, operation, :ok, _compensated_effect, state}) do
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, :abort, _compensated_effect, state}) do
    state = state(state, aborted?: true)
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result(
         {name, operation, {:retry, _retry_opts}, _compensated_effect, state(aborted?: true) = state}
       ) do
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result(
         {name, operation, {:retry, retry_opts}, _compensated_effect, state(aborted?: false) = state}
       ) do
    state(attempts: attempts) = state

    if Retries.retry_with_backoff?(attempts, retry_opts) do
      state = state(state, attempts: attempts + 1)
      {:retry_transaction, {name, operation}, state}
    else
      {:next_compensation, {name, operation}, state}
    end
  end

  defp handle_compensation_result(
         {name, operation, {:continue, effect}, _compensated_effect,
          state(last_effect_or_error: {name, _reason}, aborted?: false) = state}
       ) do
    state(effects_so_far: effects_so_far) = state
    state = state(state, last_effect_or_error: effect, effects_so_far: Map.put(effects_so_far, name, effect))
    {:next_transaction, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, {:continue, _effect}, _compensated_effect, state}) do
    {:next_compensation, {name, operation}, state}
  end

  defp handle_compensation_result({name, operation, {:raise, _} = error, compensated_effect, state}) do
    state = state(state, last_effect_or_error: error)
    {:compensation_error, {name, operation, compensated_effect}, state}
  end

  defp handle_compensation_result({name, operation, {:exit, _reason} = error, compensated_effect, state}) do
    state = state(state, last_effect_or_error: error)
    {:compensation_error, {name, operation, compensated_effect}, state}
  end

  defp handle_compensation_result({name, operation, {:throw, _error} = error, compensated_effect, state}) do
    state = state(state, last_effect_or_error: error)
    {:compensation_error, {name, operation, compensated_effect}, state}
  end

  # Shared

  defp execute_next_stage({:next_transaction, {name, operation}, state}, stages, executed_stages, attrs) do
    execute_transactions(stages, [{name, operation} | executed_stages], attrs, state)
  end

  defp execute_next_stage({:next_transaction, state}, [], [{prev_name, _prev_op} | executed_stages], attrs) do
    state(effects_so_far: effects_so_far) = state
    state = state(state, last_effect_or_error: Map.get(effects_so_far, prev_name))
    execute_transactions([], executed_stages, attrs, state)
  end

  defp execute_next_stage({:start_compensations, {name, operation}, state}, compensated_stages, stages, attrs) do
    execute_compensations(compensated_stages, [{name, operation} | stages], attrs, state)
  end

  defp execute_next_stage({:start_compensations, state}, compensated_stages, stages, attrs) do
    execute_compensations(compensated_stages, stages, attrs, state)
  end

  defp execute_next_stage({:next_compensation, {name, operation}, state}, compensated_stages, stages, attrs) do
    execute_compensations([{name, operation} | compensated_stages], stages, attrs, state)
  end

  defp execute_next_stage({:retry_transaction, {name, operation}, state}, compensated_stages, stages, attrs) do
    execute_transactions([{name, operation} | compensated_stages], stages, attrs, state)
  end

  defp execute_next_stage(
         {:compensation_error, _compensation_error, state(on_compensation_error: :raise) = state},
         _compensated_stages,
         _stages,
         _opts
       ) do
    state(last_effect_or_error: error) = state
    return_or_reraise(error)
  end

  defp execute_next_stage({:compensation_error, compensation_error, state}, _compensated_stages, stages, attrs) do
    state(
      last_effect_or_error: error,
      effects_so_far: effects_so_far,
      on_compensation_error: on_compensation_error
    ) = state

    {name, operation, compensated_effect} = compensation_error

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
        apply(on_compensation_error, :handle_error, [{:exception, exception, stacktrace}, compensations_to_run, attrs])

      {:raise, exception} ->
        apply(on_compensation_error, :handle_error, [{:exception, exception, []}, compensations_to_run, attrs])

      {:exit, reason} ->
        apply(on_compensation_error, :handle_error, [{:exit, reason}, compensations_to_run, attrs])

      {:throw, error} ->
        apply(on_compensation_error, :handle_error, [{:throw, error}, compensations_to_run, attrs])
    end
  end

  defp compensation_error_message({:raise, {exception, stacktrace}}) do
    """
    Because exception was raised:

      #{Exception.format(:error, exception, stacktrace)}

    """
  end

  defp compensation_error_message({:raise, exception}) do
    """
    Because exception was raised:

      #{Exception.format(:error, exception)}

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

  defp maybe_notify_final_hooks(result, filanlize_callbacks, attrs) do
    status = if elem(result, 0) == :ok, do: :ok, else: :error

    :ok =
      filanlize_callbacks
      |> Enum.map(fn
        {module, function, args} ->
          args = [status, attrs | args]
          {{module, function, args}, apply_and_catch_errors(module, function, args)}

        callback ->
          args = [status, attrs]
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
  defp return_or_reraise({:raise, exception}), do: raise(exception)
  defp return_or_reraise({:error, reason}), do: {:error, reason}

  defp filter_and_reraise(exception, stacktrace) do
    stacktrace =
      Enum.reject(stacktrace, &match?({__MODULE__, fun, _, _} when fun not in @stacktrace_functions_whitelist, &1))

    reraise exception, stacktrace
  end
end
