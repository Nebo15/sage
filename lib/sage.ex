defmodule Sage do
  @moduledoc """
  Documentation for Sage.

  - `before_and_after_state` is shared across all callback runs. By default: `%{execute_args: execute_args}`.

  ## Tracing your Sage

  If you want to set metrics and measure how much time each of your Sage execution steps took,
  you can use `after_transaction`, `before_transaction`, `after_compensation`, `before_compensation`
  callbacks.

  ...

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
                      | (effects_so_far :: effects() -> {:ok | :error, any()})

  @typep compensation :: {module(), atom(), [any()]}
                       | ({failed_operation :: name(), failed_value :: any(), effects_so_far :: effects()} ->
                               {:ok, any()} # I am compensated my transactions, continue backwards-recovery
                             | {:abort, any()} # I am compensated transaction and want to force backwards recovery on all steps
                             | {:retry, any()}) # I am compensated by transaction, let's retry with this data from my tx

  @typep finally :: {module(), atom(), [any()]}
                  | ({:ok | :error, effects_so_far :: effects()} -> no_return)

  @opaque t :: %__MODULE__{
    operations: [{name(), transaction(), compensation()}],
    names: MapSet.t(),
    direction: :forward | :backward | :force_backward,
    finally: finally(),
    cursor: name()
  }

  defstruct [
    operations: [],
    operation_names: MapSet.new(),
    cursor: nil,
    direction: :forward
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
  Appends sage with an cached transaction and function to compensate it's side effects.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.
  """
  @spec run_cached(sage :: t(), apply :: transaction(), rollback :: compensation(), opts :: cache_opts()) :: t()

  @doc """
  Appends sage with an asynchronous transaction and function to compensate it's side effects.

  Next non-asynchronous operation will await for this function return.
  """
  @spec run_async(sage :: t(), apply :: transaction(), rollback :: compensation()) :: t()

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
  """
  @spec checkpoint(sage :: t(), retry_opts :: retry_opts()) :: t()

  @doc """
  Appends a sage with a function that will be triggered after sage success or abort.
  """
  @spec finally(sage :: t(), apply :: finally()) :: t()

  @doc """
  Merges another sage or adds a step to call external Module to return a sage to merge at run-time.
  """
  @spec merge(sage :: t(), sage :: t() | {module(), function(), [any()]}) :: t()

  @doc """
  Creates a new sage.
  """
  @spec new() :: t()
  def new,
    do: %Sage{}
end
