defmodule Sage do
  @moduledoc ~S"""
  Sage is an implementation of [Sagas](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) pattern
  in pure Elixir. It is go to way when you dealing with distributed transactions, especially with
  an error recovery/cleanup. Sagas guarantees that either all the transactions in a saga are
  successfully completed or compensating transactions are run to amend a partial execution.

  ## Critical Error Handling

  ### For Transactions

  Transactions are wrapped in a `try..catch` block.
  Whenever a critical error occurs (exception is raised or function has an unexpected return)
  Sage will run all compensations and then reraise exception, so you would see it like it occurred without Sage.

  ### For Compensations

  By default, compensations are not protected from critical errors and would raise an exception.
  This is done to keep simplicity and follow "let it fall" pattern of the language,
  thinking that this kind of errors should be logged and then manually investigated by a developer.

  But if that's not enough for you, it is possible to register handler via `on_compensation_error/2`.
  When it's registered, compensations are wrapped in a `try..catch` block
  and then it's error handler responsibility to take care about further actions. Few solutions you might want to try:

  - Send notification to a Slack channel about need of manual resolution;
  - Retry compensation;
  - Spin off a new supervised process that would retry compensation and return an error in the Sage.
  (Useful when you have connection issues that would be resolved at some point in future.)

  Logging for compensation errors is pretty verbose to drive the attention to the problem from system maintainers.

  ## Tracing your Sage

  If you want to set metrics and measure how much time each of your Sage execution steps took,
  you can use `after_transaction`, `before_transaction`, `after_compensation`, `before_compensation`
  callbacks.

  They are receiving attrs from `execute/1` call, but state mutations won't affects Sage execution - only other
  tracing callbacks would have access to it.

  ## Examples

    def my_sage do
      import Sage

      new()
      |> run(:user, &create_user/2, &delete_user/3)
      |> run_cached(:plans, &fetch_subscription_plans/3)
      |> checkpoint(retry_limit: 3) # Retry everything after a checkpoint 3 times (if anything fails), `retry_timeout` is taken from `global_opts`
      |> run(:subscription, &create_subscription/2, delete_subscription/3)
      |> run_async(:delivery, &schedule_delivery/2, &delete_delivery_from_schedule/3)
      |> run_async(:receipt, &send_email_receipt/2, &send_excuse_for_email_receipt/3)
      |> run(:update_user, &set_plan_for_a_user/2, &rollback_plan_for_a_user/3)
      |> finally(&acknowledge_job/2)
    end

    global_state = [
      pool: Poolboy.start_link(),
      retry_limit: 10,
      retry_timeout: 100,
      after_transaction: (name, before_and_after_state -> no_return),
      before_transaction: (name, before_and_after_state -> no_return),
      after_compensation: (name, before_and_after_state -> no_return),
      before_compensation: (name, before_and_after_state -> no_return),
    ]

    my_sage()
    |> execute(global_state)
    |> case do
      {:ok, success, _effects} ->
        {:ok, success}

      {:error, reason, non_compensated_effects} ->
        Logger.error("Failed to execute with reason #{inspect(reason)}. Effects left: #{inspect(non_compensated_effects)}")
        {:error, reason}
    end

  Wrapping Sage in a transaction:

    # In this sage we don't need `&delete_user/2` and `&rollback_plan_for_a_user/3`,
    # everything is rolled back as part of DB transaction

    my_db_aware_sage()
    |> Sage.to_function(global_state)
    |> Repo.transaction()
  """
  use Application

  @typep name :: atom()

  @typep effects :: [{name(), any()}]

  @typep transaction :: {module(), atom(), [any()]}
                      | (effects_so_far :: effects(), global_state :: any() -> {:ok | :error | :abort, any()})

  @typep compensation :: {module(), atom(), [any()]}
                       | :noop # No side effects to compensate
                       | ({failed_operation :: name(), failed_value :: any(), effects_so_far :: effects()} ->
                               :ok # I am compensated my transactions, continue backwards-recovery
                             | :abort # I am compensated transaction and want to force backwards recovery on all steps
                             | {:retry, Keyword.t}) # I am compensated my transaction, let's retry with this retry opts
                             | {:continue, any()} # I am the Circuit Breaker and I know how live wit this error. Can be returned only when operation name == failed operation name

  @typep finally :: [{module(), atom(), [any()]}]
                  | [({:ok | :error} -> no_return)]

  @type t :: %__MODULE__{
    operations: [{name(), {:run, transaction(), compensation()} | {:run_async, transaction(), compensation(), timeout()}}],
    operation_names: MapSet.t(),
    on_compensation_error: :raise | module(),
    finally: finally()
  }

  defstruct [
    operations: [],
    operation_names: MapSet.new(),
    finally: [],
    on_compensation_error: :raise
  ]

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      {Task.Supervisor, name: Sage.AsyncTransactionSupervisor}
    ]

    opts = [strategy: :one_for_one, name: Sage.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Creates a new sage.
  """
  @spec new() :: t()
  def new,
    do: %Sage{}

  @doc """
  Appends sage with an transaction that does not have side effects.
  """
  @spec run(sage :: t(), name :: name(), apply :: transaction()) :: t()
  def run(sage, name, transaction),
    do: add_operation(sage, :run, name, transaction, :noop)

  @doc """
  Appends sage with an transaction and function to compensate it's side effects.
  """
  @spec run(sage :: t(), name :: name(), apply :: transaction(), rollback :: compensation()) :: t()
  def run(sage, name, transaction, compensation),
    do: add_operation(sage, :run, name, transaction, compensation)

  @doc """
  Appends sage with an asynchronous transaction and function to compensate it's side effects.

  Next non-asynchronous operation will await for this function return.

  If there is an error while one or more asynchronous operations are creating transaction,
  Sage will await for them to complete and compensate created effects.
  """
  @spec run_async(sage :: t(), name :: name(), apply :: transaction(), rollback :: compensation(), opts :: Keyword.t()) :: t()
  def run_async(sage, name, transaction, compensation, opts \\ []),
    do: add_operation(sage, :run_async, name, transaction, compensation, opts)

  @doc """
  Appends a sage with a function that will be triggered after sage success or abort.
  """
  @spec finally(sage :: t(), callback :: finally()) :: t()
  def finally(%Sage{} = sage, callback) when is_function(callback, 1),
    do: %{sage | finally: sage.finally ++ [callback]}
  def finally(%Sage{} = sage, {module, function, arguments})
    when is_atom(module) and is_atom(function) and is_list(arguments),
    do: %{sage | finally: sage.finally ++ [{module, function, arguments}]}

  @doc """
  Merges another sage or adds a step to call external Module to return a sage to merge at run-time.
  """
  @spec merge(sage :: t(), sage :: t() | {module(), function(), [any()]}) :: t()
  def merge(%Sage{}, %Sage{}), do: raise "not implemented"
  def merge(%Sage{}, {_m, _f, _a}), do: raise "not implemented"

  @doc """
  Executes a Sage with global state.
  """
  @spec execute(sage :: t(), opts :: any()) :: {:ok, result :: any(), effects :: effects()} | {:error, any()}
  def execute(%Sage{} = sage, opts) do
    sage.operations
    |> Enum.reverse()
    |> execute_transactions([], opts, {nil, %{}, {0, []}, false, []})
    |> filanlize(sage.finally)
    |> return()
  end

  defp filanlize(result, []), do: result
  defp filanlize(result, filanlize_callbacks) do
    status = if elem(result, 0) in [:exit, :raise, :error], do: :error, else: :ok
    Enum.map(filanlize_callbacks, fn
      {module, function, args} ->
        apply(module, function, [status] + args)
      callback ->
        callback.(status)
    end)
    result
  end

  defp return(result) do
    case result do
      {:ok, effect, other_effects} -> {:ok, effect, other_effects}
      {:exit, reason} -> exit(reason)
      {:raise, {exception, stacktrace}} -> reraise(exception, stacktrace)
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_transactions([{name, operation} | operations], executed_operations, opts, state) do
    {_last_effect_or_error, _effects_so_far, _retries, _abort?, tasks} = state

    {{name, operation}, state}
    |> maybe_await_for_tasks(tasks)
    |> maybe_execute_transaction(opts)
    |> handle_operation_result()
    |> execute_operation_or_compensation(operations, executed_operations, opts)
  end
  defp execute_transactions([], executed_operations, opts, state) do
    {last_effect, effects_so_far, _retries, _abort?, tasks} = state

    if tasks == [] do
      {:ok, last_effect, effects_so_far}
    else
      {:execute, state}
      |> maybe_await_for_tasks(tasks)
      |> handle_operation_result()
      |> execute_operation_or_compensation([], executed_operations, opts)
    end
  end

  # TODO: Move effects_so_far from state

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
          message = "asynchronous transaction did not return within the timeout #{to_string(timeout)}"
          {name, {:raise, {%RuntimeError{message: message}, System.stacktrace()}}}
      end
    end)
    |> Enum.reduce({operation, state}, fn
      {name, result}, {operation, state} ->
        case handle_operation_result({name, :async, result, state}) do
          {:next_operation, {^name, :async}, state} ->
            {operation, state}

          {:compensate, {^name, :async}, state} ->
            {:compensate, state}
        end
    end)
  end

  defp maybe_execute_transaction({:compensate, state}, _opts),
    do: {:compensate, state}
  defp maybe_execute_transaction({{name, operation}, state}, opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, _tasks} = state
    {name, operation, execute_transaction(operation, effects_so_far, opts), state}
  end

  defp execute_transaction({:run, operation, _compensation, []}, effects_so_far, opts) do
    try do
      case apply_transaction_fun(operation, effects_so_far, opts) do
        {:ok, effect} -> {:ok, effect}
        {:error, reason} -> {:error, reason}
        {:abort, reason} -> {:abort, reason}
        other ->
          raise RuntimeError, """
          unexpected return from transaction function #{inspect(operation)},
          expected it to be {:ok, effect}, {:error, reason} or {:abort, reason}, got:

            #{inspect(other)}
          """
      end
    rescue
      exception ->
        {:raise, {exception, System.stacktrace()}}
    catch
      :exit, reason ->
        {:exit, reason}
    end
  end
  defp execute_transaction({:run_async, operation, _compensation, tx_opts}, effects_so_far, opts) do
    task =
      Task.Supervisor.async_nolink(Sage.AsyncTransactionSupervisor, fn ->
        case apply_transaction_fun(operation, effects_so_far, opts) do
          {:ok, effect} -> {:ok, effect}
          {:error, reason} -> {:error, reason}
          {:abort, reason} -> {:abort, reason}
          other ->
            raise RuntimeError, """
            unexpected return from transaction function #{inspect(operation)},
            expected it to be {:ok, effect}, {:error, reason} or {:abort, reason}, got:

              #{inspect(other)}
            """
        end
      end)

    {task, tx_opts}
  end

  defp handle_operation_result({:compensate, state}),
    do: {:compensate, state}
  defp handle_operation_result({:execute, state}),
    do: {:execute, state}
  defp handle_operation_result({name, operation, {%Task{}, _async_opts} = async_job, state}) do
    {last_effect_or_error, effects_so_far, retries, _abort?, tasks} = state
    state = {last_effect_or_error, effects_so_far, retries, false, [{name, async_job} | tasks]}
    {:next_operation, {name, operation}, state}
  end
  defp handle_operation_result({name, operation, {:ok, effect}, state}) do
    {_last_effect_or_error, effects_so_far, retries, _abort?, []} = state
    state = {effect, Map.put(effects_so_far, name, effect), retries, false, []}
    {:next_operation, {name, operation}, state}
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

  defp execute_operation_or_compensation({:next_operation, {name, operation}, state}, operations, executed_operations, opts) do
    execute_transactions(operations, [{name, operation} | executed_operations], opts, state)
  end
  defp execute_operation_or_compensation({:compensate, {name, operation}, state}, operations, executed_operations, opts) do
    execute_compensations([{name, operation} | executed_operations], operations, opts, state)
  end
  defp execute_operation_or_compensation({:compensate, state}, operations, executed_operations, opts) do
    execute_compensations(executed_operations, operations, opts, state)
  end
  defp execute_operation_or_compensation({:execute, state}, [], [{prev_name, _prev_operation} | executed_operations], opts) do
    {_last_effect_or_error, effects_so_far, _retries, _abort?, []} = state
    state = put_elem(state, 0, Map.get(effects_so_far, prev_name))
    execute_transactions([], executed_operations, opts, state)
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
          raise "Circuit breaking is only allowed for continuing compensated transaction"
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

  defp apply_transaction_fun({mod, fun, args}, effects_so_far, opts),
    do: apply(mod, fun, [effects_so_far, opts | args])
  defp apply_transaction_fun(fun, effects_so_far, opts),
    do: apply(fun, [effects_so_far, opts])

  defp apply_compensation_fun({mod, fun, args}, effect_to_compensate, {name, reason}, opts),
    do: apply(mod, fun, [effect_to_compensate, {name, reason}, opts | args])
  defp apply_compensation_fun(fun, effect_to_compensate, {name, reason}, opts),
    do: apply(fun, [effect_to_compensate, {name, reason}, opts])

  @doc """
  Wraps `execute/2` into anonymous function to be run in a Repo transaction.
  """
  @spec to_function(sage :: t(), opts :: Keyword.t()) :: function()
  def to_function(%Sage{} = sage, opts),
    do: fn -> execute(sage, opts) end

  defp add_operation(%Sage{} = sage, type, name, transaction, compensation, opts \\ [])
      when is_atom(name)
       and (is_function(transaction, 2) or is_tuple(transaction) and tuple_size(transaction) == 3)
       and (is_function(compensation, 3) or is_tuple(compensation) and tuple_size(compensation) == 3 or compensation == :noop) do
    %{operations: operations, operation_names: names} = sage
    if MapSet.member?(names, name) do
      raise "#{inspect name} is already a member of the Sage: \n#{inspect(sage)}"
    else
      %{sage | operations: [{name, {type, transaction, compensation, opts}} | operations],
               operation_names: MapSet.put(names, name)}
    end
  end
end

# defprotocol inspect for sage
