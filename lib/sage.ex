defmodule Sage do
  @moduledoc ~S"""
  Sage is an implementation of [Sagas](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) pattern
  in pure Elixir. It is go to way when you dealing with distributed transactions, especially with
  an error recovery/cleanup. Sagas guarantees that either all the transactions in a saga are
  successfully completed or compensating transactions are run to amend a partial execution.

  ## Critical Error Handling

  ### For Transactions

  Transactions are wrapped in a `try..catch` block.
  Whenever a critical error occurs (exception is raised, function has an unexpected return or async transaction exits)
  Sage will run all compensations and then reraise the exception, so you would see it like it occurred without Sage.

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

    execute_opts = [
      pool: Poolboy.start_link(),
      retry_limit: 10,
      retry_timeout: 100,
      after_transaction: (name, before_and_after_state -> no_return),
      before_transaction: (name, before_and_after_state -> no_return),
      after_compensation: (name, before_and_after_state -> no_return),
      before_compensation: (name, before_and_after_state -> no_return),
    ]

    my_sage()
    |> execute(execute_opts)
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
    |> Sage.to_function(execute_opts)
    |> Repo.transaction()
  """
  use Application

  @type return :: {:ok, result :: any(), effects :: effects()} | {:error, any()}

  @typep name :: atom()

  @typep effects :: [{name(), any()}]

  @typep transaction :: {module(), atom(), [any()]}
                      | (effects_so_far :: effects(), execute_opts :: any() -> {:ok | :error | :abort, any()})

  @typep compensation :: {module(), atom(), [any()]}
                       | :noop # No side effects to compensate
                       | ({failed_operation :: name(), failed_value :: any(), effects_so_far :: effects()} ->
                               :ok # I am compensated my transactions, continue backwards-recovery
                             | :abort # I am compensated transaction and want to force backwards recovery on all steps
                             | {:retry, Keyword.t()}) # I am compensated my transaction, let's retry with this retry opts
                             | {:continue, any()} # I am the Circuit Breaker and I know how live wit this error. Can be returned only when operation name == failed operation name

  @typep finally :: [{module(), atom(), [any()]}]
                  | [({:ok | :error} -> no_return)]

  @typep async_opts :: [{:timeout, integer() | :infinity}]

  @typep operation :: {:run, transaction(), compensation()}
                    | {:run_async, transaction(), compensation(), async_opts()}

  @type t :: %__MODULE__{
    operations: [{name(), operation()}],
    operation_names: MapSet.t(),
    finally: finally(),
    on_compensation_error: :raise | module()
  }

  defstruct [
    operations: [],
    operation_names: MapSet.new(),
    finally: [],
    on_compensation_error: :raise
  ]

  @doc false
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
  Executes a Sage with global state.
  """
  @spec execute(sage :: t(), opts :: any()) :: return()
  defdelegate execute(sage, opts \\ []), to: Sage.Executor

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
      raise Sage.DuplicateOperationError, sage: sage, name: name
    else
      %{sage | operations: [{name, {type, transaction, compensation, opts}} | operations],
               operation_names: MapSet.put(names, name)}
    end
  end
end
