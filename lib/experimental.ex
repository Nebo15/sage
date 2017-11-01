defmodule Sage.Experimental do
  @moduledoc """
  This module described experimental features planned for Sage.
  """

  @doc """
  Appends sage with an cached transaction and function to compensate it's side effects.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.
  """
  @callback run_cached(sage :: t(), apply :: transaction(), rollback :: compensation(), opts :: cache_opts()) :: t()

  @doc """
  Appends sage with an asynchronous cached transaction and function to compensate it's side effects.

  Next non-asynchronous operation will await for this function return.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.
  """
  @callback run_async_cached(sage :: t(), apply :: transaction(), rollback :: compensation(), opts :: cache_opts()) :: t()

  @doc """
  Appends sage with an checkpoint at which forward retries should occur.

  Internally this is the same as using:

      |> run(fn _ -> {:ok, :noop}, fn state -> if adapter.retry?(state, retry_opts) do {:retry, state} else {:ok, state} end)

  TODO: Also can be used as point of synchronization, eg.
    - to persist temporary state in a DB to idempotently retry it;
    - to ack previous execution stage to MQ and create a new one.

  TODO: Rename to retry?
  """
  @callback checkpoint(sage :: t(), retry_opts :: retry_opts()) :: t()

  @doc """
  Register function that will handle all critical errors for a compensating functions.

  Internally we will wrap compensation in a try..catch block and handoff error handling to a callback function.

  This allows to implement complex rescuing strategies, eg:
    - Notify developer about need of manual resolution;
    - Retries for compensations;
    - Spin off a compensation process and return an error in the sage.
    Process can keep retrying to compensate effects by storing sage in state or a persistent storage.
  """
  @callback on_compensation_error(sage :: t(), {module(), function(), [any()]}) :: t()

  @doc """
  Register persistent adapter and idempotency key generator to make it possible to re-run same requests
  idempotently (by either replying with old success response or continuing from the latest failed Sage transaction).
  """
  @callback with_idempotency(sage :: t(), adapter :: module()) :: t()

  @doc """
  Concurrently run transaction after it's dependencies.

  Would allow to build a dependency tree and run everything with maximum concurrency.
  """
  @callback run_async_after(sage :: t(), [after_name :: name()], apply :: transaction(), rollback :: compensation()) :: t()
end
