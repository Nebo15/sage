defmodule Sage do
  @moduledoc ~S"""
  Sage is a dependency-free implementation of [Sagas](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf)
  pattern in pure Elixir. It is a go to way when you dealing with distributed transactions, especially with
  an error recovery/cleanup. Sage does it's best to guarantee that either all of the transactions in a saga are
  successfully completed or compensating that all of the transactions did run to amend a partial execution.

  This is done by defining two way flow with transaction and compensation functions. When one of the transactions
  fails, Sage will ensure that transaction's and all of it's predecessors compensations are executed. However,
  it's important to note that Sage can not protect you from a node failure that executes given Sage.

  ## Critical Error Handling

  ### For Transactions

  Transactions are wrapped in a `try..catch` block.

  Whenever a critical error occurs (exception is raised, error thrown or exit signal is received)
  Sage will run all compensations and then reraise the exception with the same stacktrace,
  so your log would look like it occurred without using a Sage.

  ### For Compensations

  By default, compensations are not protected from critical errors and would raise an exception.
  This is done to keep simplicity and follow "let it fall" pattern of the language,
  thinking that this kind of errors should be logged and then manually investigated by a developer.

  But if that's not enough for you, it is possible to register handler via `with_compensation_error_handler/2`.
  When it's registered, compensations are wrapped in a `try..catch` block
  and then it's error handler responsibility to take care about further actions. Few solutions you might want to try:

  - Send notification to a Slack channel about need of manual resolution;
  - Retry compensation;
  - Spawn a new supervised process that would retry compensation and return an error in the Sage.
  (Useful when you have connection issues that would be resolved at some point in future.)

  Logging for compensation errors is pretty verbose to drive the attention to the problem from system maintainers.

  ## `finally/2` hook

  Sage does it's best to make sure final callback is executed even if there is a program bug in the code.
  This guarantee simplifies integration with a job processing queues, you can read more about it at
  [GenTask Readme](https://github.com/Nebo15/gen_task).

  If an error is raised within `finally/2` hook, it's getting logged and ignored. Follow the simple rule - everything
  that is on your critical path should be a Sage transaction.

  ## Tracing and measuring Sage execution steps

  Sage allows you to set a tracer module which is called on each step of the execution flow (before and after
  transactions and/or compensations). It could be used to report metrics on the execution flow.

  If error is raised within tracing function, it's getting logged and ignored.
  """
  use Application

  defguardp is_mfa(mfa)
            when is_tuple(mfa) and tuple_size(mfa) == 3 and
                   (is_atom(elem(mfa, 0)) and is_atom(elem(mfa, 1)) and is_list(elem(mfa, 2)))

  @typedoc """
  Name of Sage execution stage.
  """
  @type stage_name :: term()

  @typedoc """
  Effects created on Sage execution.
  """
  @type effects :: map()

  @typedoc """
  Options for asynchronous transaction stages.
  """
  @type async_opts :: [{:timeout, integer() | :infinity}]

  @typedoc """
  Retry options.

  Retry count for all a sage execution is shared and stored internally,
  so even trough you can increase retry limit - retry count would be
  never reset to make sure that execution would not be retried infinitely.

  Available retry options:
    * `:retry_limit` - is the maximum number of possible retry attempts;
    * `:base_backoff` - is the base backoff for retries in ms, no backoff is applied if this value is nil or not set;
    * `:max_backoff` - is the maximum backoff value, default: `5_000` ms.;
    * `:enable_jitter` - whatever jitter is applied to backoff value, default: `true`;

  Sage will log and give up retrying if options are invalid.

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

  `Sage.MalformedTransactionReturnError` is raised after compensating all effects if callback returned malformed result.

  ## Transaction guidelines

  You should try to make your transactions idempotent, which makes possible to retry if compensating
  transaction itself fails. According a modern HTTP semantics, the `PUT` and `DELETE` verbs are idempotent.
  Also, some services [support idempotent requests via `idempotency keys`](https://stripe.com/blog/idempotency).
  """
  @type transaction :: (effects_so_far :: effects(), execute_opts :: any() -> {:ok | :error | :abort, any()}) | mfa()

  defguardp is_transaction(value) when is_function(value, 2) or is_mfa(value)

  @typedoc """
  Compensation callback, can either anonymous function or an `{module, function, [arguments]}` tuple.

  Receives:

     * effect created by transaction it's responsible for or `nil` in case effect is not known due to an error;
     * effects created by preceding executed transactions;
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
  (they both are parts of for the same execution step). Otherwise curcuit breaker would be ignored and Sage will
  continue applying backward recovery.

  The circuit breaker should use data which is local to the sage execution, preferably from list of options
  which are set via `execute/2` 2nd argument. This would guarantee that circuit breaker would not fail when
  response cache is not available.

  ## Retries

  After receiving a `{:retry, [retry_limit: limit]}` Sage will retry the transaction on a stage where retry was
  received.

  Take into account that by doing retires you can increase execution time and block process that executes the Sage,
  which can produce timeout, eg. when you trying to respond to an HTTP request.

  ## Compensation guidelines

  General rule is that irrespectively to what compensate wants to return, **effect must be always compensated**.
  No matter what, side effects must not be created from compensating transaction.

  > A compensating transaction doesn't necessarily return the data in the system to the state
  > it was in at the start of the original operation. Instead, it compensates for the work
  > performed by the steps that completed successfully before the operation failed.
  >
  > source: https://docs.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction

  You should try to make your compensations idempotent, which makes possible to retry if compensating
  transaction itself fails. According a modern HTTP semantics, the `PUT` and `DELETE` verbs are idempotent.
  Also, some services [support idempotent requests via `idempotency keys`](https://stripe.com/blog/idempotency).

  Compensation transactions should not rely on effects created by preceding executed transactions, otherwise
  it will be more likely that your code is not idempotent and harder to maintain. Use them only as a last
  resort.
  """
  @type compensation ::
          (effect_to_compensate :: any(), effects_so_far :: effects(), execute_opts :: any() ->
             :ok | :abort | {:retry, retry_opts :: retry_opts()} | {:continue, any()})
          | :noop
          | mfa()

  defguardp is_compensation(value) when is_function(value, 3) or is_mfa(value) or value == :noop

  @typedoc """
  Final hook.

  It receives `:ok` if all transactions are successfully completed or `:error` otherwise
  and options passed to the `execute/2`.

  Return is ignored.
  """
  @type final_hook :: (:ok | :error, execute_opts :: any() -> no_return()) | mfa()

  defguardp is_final_hook(value) when is_function(value, 2) or (is_tuple(value) and tuple_size(value) == 3)

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
  Creates a new Sage.
  """
  @spec new() :: t()
  def new, do: %Sage{}

  @doc """
  Register error handler for compensations.

  Adapter must implement `Sage.CompensationErrorHandler` behaviour.

  For more information see "Critical Error Handling" in the module doc.
  """
  @spec with_compensation_error_handler(sage :: t(), module :: module()) :: t()
  def with_compensation_error_handler(%Sage{} = sage, module) when is_atom(module) do
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
  def with_tracer(%Sage{} = sage, module) when is_atom(module) do
    %{tracers: tracers} = sage

    if MapSet.member?(tracers, module) do
      raise Sage.DuplicateTracerError, sage: sage, module: module
    end

    %{sage | tracers: MapSet.put(tracers, module)}
  end

  @doc """
  Appends the Sage with a function that will be triggered after Sage execution.

  Registering duplicated final hook is not allowed and would raise
  an `Sage.DuplicateFinalHookError` exception.

  For hook specification see `t:final_hook/0`.
  """
  @spec finally(sage :: t(), hook :: final_hook()) :: t()
  def finally(%Sage{} = sage, hook) when is_final_hook(hook) do
    %{final_hooks: final_hooks} = sage

    if MapSet.member?(final_hooks, hook) do
      raise Sage.DuplicateFinalHookError, sage: sage, hook: hook
    end

    %{sage | final_hooks: MapSet.put(final_hooks, hook)}
  end

  @doc """
  Appends sage with a transaction and function to compensate it's effect.

  Raises `Sage.DuplicateStageError` exception if stage name is duplicated for a given sage.

  ### Callbacks

  Callbacks can be either anonymous function or an `{module, function, [arguments]}` tuple.
  For callbacks interface see `t:transaction/0` and `t:compensation/0` type docs.

  ### Noop compensation

  If transaction does not produce effect to compensate, pass `:noop` instead of compensation
  callback or use `run/3`.
  """
  @spec run(sage :: t(), name :: stage_name(), transaction :: transaction(), compensation :: compensation()) :: t()
  def run(sage, name, transaction, compensation),
    do: add_stage(sage, name, build_operation!(:run, transaction, compensation))

  @doc """
  Appends sage with a transaction that does not have side effect.

  This is an alias for calling `run/4` with a `:noop` instead of compensation callback.
  """
  @spec run(sage :: t(), name :: stage_name(), transaction :: transaction()) :: t()
  def run(sage, name, transaction),
    do: add_stage(sage, name, build_operation!(:run, transaction, :noop))

  @doc """
  Appends sage with an asynchronous transaction and function to compensate it's effect.

  Asynchronous transactions are awaited before the next synchronous transaction or in the end
  of sage execution. If there is an error in asynchronous transaction, Sage will await for other
  transactions to complete or fail and then compensate for all the effect created by them.

  # Callbacks

  Transaction callback for asynchronous stages receives only effects created by preceding
  synchronous transactions.

  For more details see `run/4`.

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
  def run_async(sage, name, transaction, compensation, opts \\ []),
    do: add_stage(sage, name, build_operation!(:run_async, transaction, compensation, opts))

  @doc """
  Executes a Sage.

  Optionally, you can pass global options in `opts`, that will be sent to
  all transaction, compensation functions and hooks. It is especially useful when
  you want to have keep sage definitions declarative and execute them with
  different arguments (eg. you may build your Sage struct in a module attribute,
  because there is no need to repeat this work for each execution).

  If there was an exception, throw or exit in one of transaction functions,
  Sage will reraise it after compensating all effects.

  For handling exceptions in compensation functions see "Critical Error Handling" in module doc.

  Raises `Sage.EmptyError` if Sage does not have any transactions.
  """
  @spec execute(sage :: t(), opts :: any()) :: {:ok, result :: any(), effects :: effects()} | {:error, any()}
  defdelegate execute(sage, opts \\ []), to: Sage.Executor

  @doc false
  @deprecated "Sage.to_function/2 was deprecated. Use Sage.transaction/3 instead."
  @spec to_function(sage :: t(), opts :: any()) :: function()
  def to_function(%Sage{} = sage, opts), do: fn -> execute(sage, opts) end

  @doc """
  Executes Sage with `Ecto.Repo.transaction/1`.

  Transaction is rolled back on error.

  Ecto must be included as application dependency.
  """
  @since "0.3.3"
  @spec transaction(sage :: t(), repo :: module(), opts :: any()) ::
          {:ok, result :: any(), effects :: effects()} | {:error, any()}
  def transaction(%Sage{} = sage, repo, opts \\ [], transaction_options \\ []) do
    return =
      repo.transaction(fn ->
        case execute(sage, opts) do
          {:ok, result, effects} -> {:ok, result, effects}
          {:error, reason} -> repo.rollback(reason)
        end
      end, transaction_options)

    case return do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

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

  # Inline functions for performance optimization
  # @compile {:inline, build_operation!: 2, build_operation!: 3, build_operation!: 4}
  defp build_operation!(:run_async, transaction, compensation, opts)
       when is_transaction(transaction) and is_compensation(compensation),
       do: {:run_async, transaction, compensation, opts}

  defp build_operation!(:run, transaction, compensation)
       when is_transaction(transaction) and is_compensation(compensation),
       do: {:run, transaction, compensation, []}
end
