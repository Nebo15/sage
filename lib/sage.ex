defmodule Sage do
  @moduledoc """
  Documentation for Sage.

  ## Extensibility

  Other modules can have protocol implementations for `Sage`, eg:

    defimpl Sage, for: MyHTTPClient do


    end


    Example extensions:

    |> run_request(:post, fn )

  ## Benefits

  ### vs `with`

  ## Handling errors

  ### For transaction callback

  ### For compensating function

  What we could do:

  1. Give up. It's essentially to let it fail and let application developer to find this error in logs and fix manually.

  2. Retry. We can reduce amount of manual work by keeping retrying to compensate transaction.
  Benefits of this approach is not clear. It application has a bug - retries won't help. If connection went down - we need very large timeouts
  and sometimes internet connection won't recover at all.

  3. Spin off a compensating process and return error. Implement steps 1 or 2 in this process.



  What if Internet connection went down and compensations are failing?

  One possible solution 1s to make use of software fault tolerant techniques along
  the lmes of recovery blocks [Ande8la,Horn74a] A recovery block 1s
  an alternate or secondary block of code that 1s provided m case a
  failure 1s detected m the primary block If a failure IS detected the
  system 1s reset to Its pre-primary state and the secondary block 1s executed

  The secondary block 1s designed to achieve the same end as the primary usmg
  a different algorithm or technique, hopefully avoiding the primary’s failure


  The other possible solution to this problem
1s manual mterventlon The erroneous transaction
1s first aborted Then It 1s given to an apphcation
programmer who, given a descrlptlon of
the error, can correct it The SEC (or the apphcation)
then reruns the transactlon and contmues
processing the saga
Fortunately, while the transaction 1s bemg
manually repalred the saga does not hold any
database resources (1 e , locks) Hence, the fact
that an already long saga ~111 take even longer
will not slgmficantly affect performance of other
transactions

  ## Tracing your Sage

  If you want to set metrics and measure how much time each of your Sage execution steps took,
  you can use `after_transaction`, `before_transaction`, `after_compensation`, `before_compensation`
  callbacks.

  ...

  - `before_and_after_state` is shared across all callback runs. By default: `%{execute_args: execute_args}`.

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

    # If you want to use DB transaction instead of certain compensations:

    # In this sage we don't need `&delete_user/2` and `&rollback_plan_for_a_user/3`,
    # everything is rolled back as part of DB transaction
    my_db_aware_sage()
    |> Sage.to_function(global_state)
    |> Repo.transaction()
  """

  @typep name :: atom()

  @typep effects :: [{name(), any()}]

  @typep transaction :: {module(), atom(), [any()]}
                      | (effects_so_far :: effects(), global_state :: any() -> {:ok | :error, any()})

  @typep compensation :: {module(), atom(), [any()]}
                       | :noop # No side effects to compensate
                       | ({failed_operation :: name(), failed_value :: any(), effects_so_far :: effects()} ->
                               {:ok, any()} # I am compensated my transactions, continue backwards-recovery
                             | {:abort, any()} # I am compensated transaction and want to force backwards recovery on all steps
                             | {:retry, any()}) # I am compensated by transaction, let's retry with this data from my tx

  @typep finally :: {module(), atom(), [any()]}
                  | ({:ok | :error, effects_so_far :: effects()} -> no_return)

  @opaque t :: %__MODULE__{
    operations: [{:run | :run_async, name(), transaction(), compensation()}],
    operation_names: MapSet.t(),
    direction: :forward | :backward | :force_backward,
    finally: finally(),
    current_operation: {:transaction, name()}
  }

  defstruct [
    operations: [],
    operation_names: MapSet.new(),
    direction: :forward,
    finally: :noop,
    on_compensation_error: :raise,
    current_operation: nil
  ]

  @typep cache_opts :: [{:adapter, module()}]

  @typep retry_opts :: [{:adapter, module()},
                        {:retry_limit, integer()},
                        {:retry_timeout, integer()}]

  @doc """
  Appends sage with an transaction that does not have side effects.
  """
  @spec run(sage :: t(), apply :: transaction()) :: t()

  @doc """
  Appends sage with an transaction and function to compensate it's side effects.
  """
  @spec run(sage :: t(), apply :: transaction(), rollback :: compensation()) :: t()

  @doc """
  Appends sage with an asynchronous transaction and function to compensate it's side effects.

  Next non-asynchronous operation will await for this function return.
  """
  @spec run_async(sage :: t(), apply :: transaction(), rollback :: compensation()) :: t()

  @doc """
  Appends a sage with a function that will be triggered after sage success or abort.
  """
  @spec finally(sage :: t(), apply :: finally()) :: t()

  @doc """
  Merges another sage or adds a step to call external Module to return a sage to merge at run-time.
  """
  @spec merge(sage :: t(), sage :: t() | {module(), function(), [any()]}) :: t()

  @doc """
  Executes a Sage with global state.
  """
  @spec execute(sage :: t(), opts :: Keyword.t())

  @doc """
  Wraps `execute/2` into anonymous function to be run in a Repo transaction.
  """
  @spec to_function(sage :: t(), opts :: Keyword.t())

  @doc """
  Creates a new sage.
  """
  @spec new() :: t()
  def new,
    do: %Sage{}
end

defmodule Sage.Experimental do
  @doc """
  Appends sage with an cached transaction and function to compensate it's side effects.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.
  """
  @spec run_cached(sage :: t(), apply :: transaction(), rollback :: compensation(), opts :: cache_opts()) :: t()

  @doc """
  Appends sage with an asynchronous cached transaction and function to compensate it's side effects.

  Next non-asynchronous operation will await for this function return.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.
  """
  @spec run_async_cached(sage :: t(), apply :: transaction(), rollback :: compensation(), opts :: cache_opts()) :: t()

  @doc """
  Appends sage with an checkpoint at which forward retries should occur.

  Internally this is the same as using:

      |> run(fn _ -> {:ok, :noop}, fn state -> if adapter.retry?(state, retry_opts) do {:retry, state} else {:ok, state} end)

  TODO: Also can be used as point of synchronization, eg.
    - to persist temporary state in a DB to idempotently retry it;
    - to ack previous execution stage to MQ and create a new one.

  TODO: Rename to retry?
  """
  @spec checkpoint(sage :: t(), retry_opts :: retry_opts()) :: t()

  @doc """
  Register function that will handle all critical errors for a compensating functions.

  Internally we will wrap compensation in a try..catch block and handoff error handling to a callback function.

  This allows to implement complex rescuing strategies, eg:
    - Notify developer about need of manual resolution;
    - Retries for compensations;
    - Spin off a compensation process and return an error in the sage.
    Process can keep retrying to compensate effects by storing sage in state or a persistent storage.
  """
  @spec on_compensation_error(sage :: t(), {module(), function(), [any()]}) :: t()

  @doc """
  Register persistent adapter and idempotency key generator to make it possible to re-run same requests
  idempotently (by either replying with old success response or continuing from the latest failed Sage transaction).
  """
  @spec with_idempotency(sage :: t(), adapter :: module()) :: t()
end
