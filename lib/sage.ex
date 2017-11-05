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

  ## Examples

    def my_sage do
      import Sage

      new()
      |> run(:user, &create_user/2, &delete_user/3)
      |> run(:plans, &fetch_subscription_plans/3)
      |> run(:subscription, &create_subscription/2, delete_subscription/3)
      |> run_async(:delivery, &schedule_delivery/2, &delete_delivery_from_schedule/3)
      |> run_async(:receipt, &send_email_receipt/2, &send_excuse_for_email_receipt/3)
      |> run(:update_user, &set_plan_for_a_user/2, &rollback_plan_for_a_user/3)
      |> finally(&acknowledge_job/2)
    end

    my_sage()
    |> execute([pool: Poolboy.start_link(), user_attrs: %{"email" => "foo@bar.com"}])
    |> case do
      {:ok, success, _effects} ->
        {:ok, success}

      {:error, reason} ->
        Logger.error("Failed to execute with reason #{inspect(reason)}")
        {:error, reason}
    end

  Wrapping Sage in a transaction:

    # In this sage we don't need `&delete_user/2` and `&rollback_plan_for_a_user/3`,
    # everything is rolled back as part of DB transaction
    def my_db_aware_sage do
      import Sage

      new()
      |> run(:user, &create_user/2)
      |> run(:plans, &fetch_subscription_plans/3)
      |> run(:subscription, &create_subscription/2, delete_subscription/3)
      |> run_async(:delivery, &schedule_delivery/2, &delete_delivery_from_schedule/3)
      |> run_async(:receipt, &send_email_receipt/2, &send_excuse_for_email_receipt/3)
      |> run(:update_user, &set_plan_for_a_user/2)
      |> finally(&acknowledge_job/2)
    end

    my_db_aware_sage()
    |> Sage.to_function(execute_opts)
    |> Repo.transaction()
  """
  use Application

  @type name :: atom()

  @type effects :: map()

  @type async_opts :: [{:timeout, integer() | :infinity}]

  @typedoc """
  Transaction callback.

  It receives effects created by preceding transactions (only synchronous ones if the transaction is asynchronous),
  and options passed to `execute/2` function.

  It should return `{:ok, effect}` if transaction is successfully completed,
  `{:error, reason}` if there was an error or `{:abort, reason}` if there was an unrecoverable error.

  After receiving `{:abort, reason}` Sage will compensate all side effects created so far and ignore all retries.

  `Sage.MalformedTransactionReturnError` is raised if callback returns malformed result.
  """
  @type transaction :: (effects_so_far :: effects(), execute_opts :: any() -> {:ok | :error | :abort, any()}) | mfa()

  @typedoc """
  Compensation callback.

  Receives effect created by transaction it's responsible for,
  `{operation_operation_name, reason}` tuple and options passed to `execute/2` function.

  It should return:

    * `:ok` if effect is compensated, Sage will continue to compensate other effects;
    * `:abort` if effect is compensated but should not be created again, \
    Sage will compensate other effects and ignore all retries;
    * `{:retry, retry_opts}` if effect is compensated but transaction can be retried with options `retry_opts`;
    * `{:continue, effect}` if effect is compensated and execution can be retried with other effect \
    to replace the transaction return. This allows to implement circuit breaker.

  General rule is that irrespectively to what compensate wants to return, **effect must be always compensated**.
  No matter what, it should not create other effects. For circuit breaker always use data that already exists,
  preferably by passing it in opts to the `execute/2`.

  ## Circuit Breaker

  After receiving a circuit breaker response Sage will continue executing transactions by using returned effect.
  Circuit breaking is only allowed if compensation function that returns it is responsible for the failed transaction
  (they both are part of for the same operation).

  If compensation violates this rule, `Sage.UnexpectedCircuitBreakError` is returned.

  It's the developer responsibility to match operation name and failed operation name.
  """
  @type compensation ::
          (effect_to_compensate() :: any(),
           {operation_operation_name :: name(), failed_value :: any()},
           execute_opts :: any() ->
             :ok | :abort | {:retry, Keyword.t()} | {:continue, any()})
          | :noop
          | mfa()
  @typedoc """
  Final callback.

  It receives `:ok` if all transactions are successfully completed or `:error` otherwise
  and options passed to the `execute/2`.
  """
  @type finally :: (:ok | :error, execute_opts :: any() -> no_return() | any()) | mfa()

  @typedoc """
  Hook which can executed before and after transaction or compensation.

  Function return is ignored. All exit's and raises are rescued and logged.

  ## Hooks State

  All hooks share their state, which by default contains options passed to `execute/2` function.
  Altering this state won't affect Sage execution in any way, changes would be visible only to other
  before and after hooks.
  """
  @type before_after_hook :: (operation_name :: name(), shared_state :: any() -> no_return() | any())

  @typep operation :: {:run, transaction(), compensation()} | {:run_async, transaction(), compensation(), async_opts()}

  @type t :: %__MODULE__{
          operations: [{name(), operation()}],
          operation_names: MapSet.t(),
          finally: [finally()],
          on_compensation_error: :raise | module()
        }

  defstruct operations: [],
            operation_names: MapSet.new(),
            finally: [],
            on_compensation_error: :raise

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
  def new, do: %Sage{}

  @doc """
  Register error handler for compensation function.

  Adapter must implement `Sage.CompensationErrorHandlerAdapter` behaviour.
  """
  @spec on_compensation_error(sage :: t(), adapter :: module()) :: t()
  def on_compensation_error(%Sage{} = sage, adapter) do
    unless Code.ensure_loaded?(adapter) or function_exported?(adapter, :handle_error, 2) do
      message = """
      adapter for compensation error handing is not defined
      or does not implement handle_error/2 function
      """

      raise ArgumentError, message
    end

    %{sage | on_compensation_error: adapter}
  end

  @doc """
  Appends sage with an transaction and function to compensate it's effects.

  Callbacks can be either anonymous function or an `{module, function, [arguments]}` tuple.
  For callbacks interface see `t:transaction/0` and `t:compensation/0` type docs.

  If transaction does not produce effects to compensate, pass `:noop` instead of compensation callback.
  """
  @spec run(sage :: t(), name :: name(), apply :: transaction(), rollback :: compensation()) :: t()
  def run(sage, name, transaction, compensation), do: add_operation(sage, :run, name, transaction, compensation)

  @doc """
  Appends sage with an transaction that does not have side effects.

  This is an alias for calling `run/4` with a `:noop` instead of compensation callback.

  Callbacks can be either anonymous function or an `{module, function, [arguments]}` tuple.
  For callbacks interface see `t:transaction/0` and `t:compensation/0` type docs.
  """
  @spec run(sage :: t(), name :: name(), apply :: transaction()) :: t()
  def run(sage, name, transaction), do: add_operation(sage, :run, name, transaction, :noop)

  @doc """
  Appends sage with an asynchronous transaction and function to compensate it's effects.
  It's transaction callback would receive only effects created by preceding synchronous transactions.

  All asynchronous transactions are awaited before next synchronous transaction.
  If there is an error in asynchronous transaction, Sage will await for other transactions to complete or fail and
  then compensate for all the effects created by them.

  Callbacks can be either anonymous function or an `{module, function, [arguments]}` tuple.
  For callbacks interface see `t:transaction/0` and `t:compensation/0` type docs.

  ## Options

    * `:timeout` - the time in milliseconds to wait for the transaction to finish, \
    `:infinity` will wait indefinitely (default: 5000);
  """
  @spec run_async(sage :: t(), name :: name(), apply :: transaction(), rollback :: compensation(), opts :: async_opts()) ::
          t()
  def run_async(sage, name, transaction, compensation, opts \\ []),
    do: add_operation(sage, :run_async, name, transaction, compensation, opts)

  @doc """
  Appends a sage with a function that will be triggered after sage success or abort.

  For callback specification see `t:finally/0`.
  """
  @spec finally(sage :: t(), callback :: finally()) :: t()
  def finally(%Sage{} = sage, callback) when is_function(callback, 2), do: %{sage | finally: sage.finally ++ [callback]}

  def finally(%Sage{} = sage, {module, function, arguments})
      when is_atom(module) and is_atom(function) and is_list(arguments),
      do: %{sage | finally: sage.finally ++ [{module, function, arguments}]}

  @doc """
  Executes a Sage.

  Optionally, you can pass global options in `opts`, that will be sent to
  all transaction and compensation functions. It is especially useful when
  you want to have keep sage definitions declarative and execute them with
  different arguments (eg. by storing it in the module attribute).

  If there was an exception or exit in one of transaction functions,
  Sage will reraise it after compensating all effects.

  For handling exceptions in compensation functions see "Critical Error Handling" in module doc.

  ## Tracing Hooks

  It is possible to set hooks that run before after each execution of a Sage operation,
  which can be used to set metrics and measure how much time each those steps took.

  Hooks are passed in `opts`:

    * `before_transaction` - called before transaction is triggered;
    * `after_transaction` - called after transaction is completed. \
    For asynchronous operations it will be called after yielding for operation results;
    * `before_compensation` - called before compensation is triggered;
    * `after_compensation` - called after compensation is completed.

  For hooks interface see `t:before_after_hook/0` type doc.
  """
  @spec execute(sage :: t(), opts :: any()) :: {:ok, result :: any(), effects :: effects()} | {:error, any()}
  defdelegate execute(sage, opts \\ []), to: Sage.Executor

  @doc """
  Wraps `execute/2` into anonymous function to be run in a Repo transaction.
  """
  @spec to_function(sage :: t(), opts :: Keyword.t()) :: function()
  def to_function(%Sage{} = sage, opts), do: fn -> execute(sage, opts) end

  defp add_operation(%Sage{} = sage, type, name, transaction, compensation, opts \\ [])
       when is_atom(name) and (is_function(transaction, 2) or (is_tuple(transaction) and tuple_size(transaction) == 3)) and
              (is_function(compensation, 3) or (is_tuple(compensation) and tuple_size(compensation) == 3) or
                 compensation == :noop) do
    %{operations: operations, operation_names: names} = sage

    if MapSet.member?(names, name) do
      raise Sage.DuplicateOperationError, sage: sage, name: name
    else
      %{
        sage
        | operations: [{name, {type, transaction, compensation, opts}} | operations],
          operation_names: MapSet.put(names, name)
      }
    end
  end
end
