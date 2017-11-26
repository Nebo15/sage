defmodule Sage do
  @moduledoc ~S"""
  Sage is an implementation of [Sagas](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) pattern
  in pure Elixir.

  It is go to way when you dealing with distributed transactions, especially with
  an error recovery/cleanup. Sagas guarantees that either all the transactions in a saga are
  successfully completed or compensating transactions are run to amend a partial execution.

  ## Critical Error Handling

  ### For Transactions

  Transactions are wrapped in a `try..catch` block.
  Whenever a critical error occurs Sage will run all compensations and then return exactly
  the same error, so you would see it like it occurred without Sage.

  ### For Compensations

  By default, compensations are not protected from critical errors and would raise an exception.
  This is done to keep simplicity and follow "let it fall" pattern of the language,
  thinking that this kind of errors should be logged and then manually investigated by a developer.

  But if that's not enough for you, it is possible to register handler via `with_compensation_error_handler/2`.
  When it's registered, compensations are wrapped in a `try..catch` block
  and then it's error handler responsibility to take care about further actions.

  Logging for compensation errors is verbose to drive the attention to the problem from system maintainers.

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

  @typedoc """
  Name of Sage execution stage.
  """
  @type stage_name :: atom()

  @typedoc """
  Effects created on Sage execution.
  """
  @type effects :: map()

  @typedoc """
  Options for asynchronous transactions.
  """
  @type async_opts :: [{:timeout, integer() | :infinity}]

  @typedoc """
  Retry options.

  Sage internally stores count of retries for whole Sage execution,
  the value is shared across all compensations so it is possible to retry
  with on various options, but with making sure that transactions won't be
  retried infinitely.

  Available retry options:
    * `:retry_limit` - is the maximum number of possible retry attempts;
    * `:base_backoff` - is the base backoff for retries in ms, no backoff is applied if this value is nil or not set;
    * `:max_backoff` - is the maximum backoff value, default: `5_000` ms.;
    * `:enable_jitter` - whatever jitter is applied to backoff value, default: `true`;

  Sage will log and ignore if options are invalid.

  ## Backoff calculation

  For exponential backoff this formula is used:

  ```
  min(max_backoff, (base_backoff * 2) ^ retry_count)
  ```

  Example:

  | Attempt | Base Backoff | Max Backoff | Sleep time |
  |---------|--------------|-------------|----------------|
  | 1       | 10           | 30000       | 20 |
  | 2       | 10           | 30000       | 400 |
  | 3       | 10           | 30000       | 8000 |
  | 4       | 10           | 30000       | 30000 |
  | 5       | 10           | 30000       | 30000 |

  When jitter is enabled backoff value is randomized:

  ```
  random(0, min(max_backoff, (base_backoff * 2) ^ retry_count))
  ```

  Example:

  | Attempt | Base Backoff | Max Backoff | Sleep interval |
  |---------|--------------|-------------|----------------|
  | 1       | 10           | 30000       | 0..20 |
  | 2       | 10           | 30000       | 0..400 |
  | 3       | 10           | 30000       | 0..8000 |
  | 4       | 10           | 30000       | 0..30000 |
  | 5       | 10           | 30000       | 0..30000 |

  For more reasoning behind using jitter, check out
  [this blog post](https://aws.amazon.com/ru/blogs/architecture/exponential-backoff-and-jitter/).
  """
  @type retry_opts :: [
          {:retry_limit, pos_integer()},
          {:base_backoff, pos_integer() | nil},
          {:max_backoff, pos_integer()},
          {:enable_jitter, boolean()}
        ]

  @typedoc """
  Transaction callback, can either anonymous function or an `{module, function, [arguments]}` tuple.

  Receives effects created by preceding executed transactions and options passed to `execute/2` function.

  Returns `{:ok, effect}` if transaction is successfully completed, `{:error, reason}` if there was an error
  or `{:abort, reason}` if there was an unrecoverable error. On receiving `{:abort, reason}` Sage will
  compensate all side effects created so far and ignore all retries.

  `Sage.MalformedTransactionReturnError` is raised if callback returns malformed result.

  ## Transaction guidelines

  Transaction function should be as idempotent as possible, since it is possible that compensation would
  retry the failed operation after compensating created side effects.
  """
  @type transaction :: (effects_so_far :: effects(), execute_opts :: any() -> {:ok | :error | :abort, any()}) | mfa()

  @typedoc """
  Compensation callback, can either anonymous function or an `{module, function, [arguments]}` tuple.

  Receives:

     * effect created by transaction it's responsible for or `nil` in case effect can not be captured;
     * `{stage_name, reason}` tuple with failed transaction name and it's failure reason;
     * options passed to `execute/2` function.

  Returns:

    * `:ok` if effect is compensated, Sage will continue to compensate other effects;
    * `:abort` if effect is compensated but should not be created again, \
    Sage will compensate other effects and ignore all retries;
    * `{:retry, retry_opts}` if effect is compensated but transaction can be retried with options `retry_opts`;
    * `{:continue, effect}` if effect is compensated and execution can be retried with other effect \
    to replace the transaction return. This allows to implement circuit breaker.

  ## Circuit Breaker

  After receiving a circuit breaker response Sage will continue executing transactions by using returned effect.

  Circuit breaking is only allowed if compensation function that returns it is responsible for the failed transaction
  (they both are parts of for the same execution step). Otherwise execution would be aborted
  and `Sage.UnexpectedCircuitBreakError` is raised. It's the developer responsibility to match operation name
  and failed operation name.

  ## Retries

  After receiving a `{:retry, [retry_limit: limit]}` Sage will retry the transaction on a stage where retry was
  received.

  Take into account that by doing retires you can increase execution time and block process that executes the Sage,
  which can produce timeout, eg. when you trying to respond to an HTTP request.

  ## Compensation guidelines

  General rule is that irrespectively to what compensate wants to return, **effect must be always compensated**.
  No matter what, it should not create other effects. For circuit breaker always use data that already exists,
  preferably by passing it in opts to the `execute/2`.

  > You should define the steps in a compensating transaction as idempotent commands.
  > This enables the steps to be repeated if the compensating transaction itself fails.
  >
  > A compensating transaction doesn't necessarily return the data in the system to the state
  > it was in at the start of the original operation. Instead, it compensates for the work
  > performed by the steps that completed successfully before the operation failed.
  >
  > source: https://docs.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction
  """
  @type compensation ::
          (effect_to_compensate() :: any(),
           {failed_stage_name :: stage_name(), failed_value :: any()},
           execute_opts :: any() ->
             :ok | :abort | {:retry, retry_opts :: retry_opts()} | {:continue, any()})
          | :noop
          | mfa()

  @typedoc """
  Final hook.

  It receives `:ok` if all transactions are successfully completed or `:error` otherwise
  and options passed to the `execute/2`.

  Return is ignored.
  """
  @type final_hook :: (:ok | :error, execute_opts :: any() -> no_return()) | mfa()

  @typep operation :: {:run | :run_async, transaction(), compensation(), Keyword.t()}

  @typep stage :: {name :: stage_name(), operation :: operation()}

  @type t :: %__MODULE__{
          stages: [stage()],
          stage_names: MapSet.t(),
          final_hooks: MapSet.t(final_hook()),
          on_compensation_error: :raise | module(),
          tracers: MapSet.t(module())
        }

  defstruct stages: [],
            stage_names: MapSet.new(),
            final_hooks: MapSet.new(),
            on_compensation_error: :raise,
            tracers: MapSet.new()

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
  Register error handler for compensations.

  Adapter must implement `Sage.CompensationErrorHandler` behaviour.

  For more information see "Critical Error Handling" in the module doc.
  """
  @spec with_compensation_error_handler(sage :: t(), module :: module()) :: t()
  def with_compensation_error_handler(%Sage{} = sage, module) do
    %{sage | on_compensation_error: module}
  end

  @doc """
  Registers tracer for a Sage execution.

  Registering duplicated tracing callback is not allowed and would raise an
  `Sage.DuplicateTracerError` exception.

  All errors during execution of a tracing callbacks would be logged,
  but it won't affect Sage execution.

  Tracing module must implement `Sage.Tracer` behaviour.
  For more information see `c:Sage.Tracer.handle_event/3`.
  """
  @spec with_tracer(sage :: t(), module :: module()) :: t()
  def with_tracer(%Sage{} = sage, module) do
    raise_on_duplicate_tracer!(sage, module)
    %{sage | tracers: MapSet.put(sage.tracers, module)}
  end

  defp raise_on_duplicate_tracer!(%{tracers: tracers} = sage, module) do
    if MapSet.member?(tracers, module) do
      raise Sage.DuplicateTracerError, sage: sage, module: module
    end
  end

  @doc """
  Appends the Sage with a function that will be triggered after Sage execution.

  Registering duplicated final hook is not allowed and would raise
  an `Sage.DuplicateFinalHookError` exception.

  For hook specification see `t:final_hook/0`.
  """
  @spec finally(sage :: t(), hook :: final_hook()) :: t()
  def finally(%Sage{} = sage, hook) when is_function(hook, 2) do
    raise_on_duplicate_final_hook!(sage, hook)
    %{sage | final_hooks: MapSet.put(sage.final_hooks, hook)}
  end

  def finally(%Sage{} = sage, {module, function, arguments} = mfa)
      when is_atom(module) and is_atom(function) and is_list(arguments) do
    raise_on_duplicate_final_hook!(sage, mfa)
    %{sage | final_hooks: MapSet.put(sage.final_hooks, mfa)}
  end

  defp raise_on_duplicate_final_hook!(%{final_hooks: final_hooks} = sage, hook) do
    if MapSet.member?(final_hooks, hook) do
      raise Sage.DuplicateFinalHookError, sage: sage, hook: hook
    end
  end

  @doc """
  Appends sage with a transaction and function to compensate it's effect.

  Callbacks can be either anonymous function or an `{module, function, [arguments]}` tuple.
  For callbacks interface see `t:transaction/0` and `t:compensation/0` type docs.

  If transaction does not produce effect to compensate,
  pass `:noop` instead of compensation callback or use `run/3`.
  """
  @spec run(sage :: t(), name :: stage_name(), transaction :: transaction(), compensation :: compensation()) :: t()
  def run(sage, name, transaction, compensation) when is_atom(name),
    do: add_stage(sage, name, build_operation!(:run, transaction, compensation))

  @doc """
  Appends sage with a transaction that does not have side effect.

  This is an alias for calling `run/4` with a `:noop` instead of compensation callback.

  Callbacks can be either anonymous function or an `{module, function, [arguments]}` tuple.
  For callbacks interface see `t:transaction/0` and `t:compensation/0` type docs.
  """
  @spec run(sage :: t(), name :: stage_name(), transaction :: transaction()) :: t()
  def run(sage, name, transaction) when is_atom(name),
    do: add_stage(sage, name, build_operation!(:run, transaction, :noop))

  @doc """
  Appends sage with an asynchronous transaction and function to compensate it's effect.

  It's transaction callback would receive only effect created by preceding synchronous transactions.

  All asynchronous transactions are awaited before next synchronous transaction.
  If there is an error in asynchronous transaction, Sage will await for other transactions to complete or fail and
  then compensate for all the effect created by them.

  Callbacks can be either anonymous function or an `{module, function, [arguments]}` tuple.
  For callbacks interface see `t:transaction/0` and `t:compensation/0` type docs.

  ## Options

    * `:timeout` - the time in milliseconds to wait for the transaction to finish, \
    `:infinity` will wait indefinitely (default: 5000);
  """
  @spec run_async(
          sage :: t(),
          name :: stage_name(),
          transaction :: transaction(),
          compensation :: compensation(),
          opts :: async_opts()
        ) :: t()
  def run_async(sage, name, transaction, compensation, opts \\ []) when is_atom(name),
    do: add_stage(sage, name, build_operation!(:run_async, transaction, compensation, opts))

  @doc """
  Executes a Sage.

  Optionally, you can pass global options in `opts`, that will be sent to
  all transaction, compensation functions and hooks. It is especially useful when
  you want to have keep sage definitions declarative and execute them with
  different arguments (eg. by building it in the module attribute).

  If there was an exception, throw or exit in one of transaction functions,
  Sage will reraise it after compensating all effects.

  For handling exceptions in compensation functions see "Critical Error Handling" in module doc.

  Raises `Sage.EmptyError` if Sage does not have any transactions.
  """
  @spec execute(sage :: t(), opts :: any()) :: {:ok, result :: any(), effects :: effects()} | {:error, any()}
  defdelegate execute(sage, opts \\ []), to: Sage.Executor

  @doc """
  Wraps `execute/2` into anonymous function to be run with a `Ecto.Repo.transaction/1`.
  """
  @spec to_function(sage :: t(), opts :: any()) :: function()
  def to_function(%Sage{} = sage, opts), do: fn -> execute(sage, opts) end

  defp add_stage(sage, name, operation) do
    %{stages: stages, stage_names: names} = sage

    if MapSet.member?(names, name) do
      raise Sage.DuplicateStageError, sage: sage, name: name
    else
      %{
        sage
        | stages: [{name, operation} | stages],
          stage_names: MapSet.put(names, name)
      }
    end
  end

  defp build_operation!(type, transaction, compensation, opts \\ []) when type in [:run, :run_async],
    do: {type, transaction, compensation, opts}
end
