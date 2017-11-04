defmodule Sage.Experimental do
  @moduledoc """
  This module described experimental features planned for Sage.
  """

  @typep cache_opts :: [{:adapter, module()}]

  @typep retry_opts :: [{:adapter, module()},
                        {:retry_limit, integer()},
                        {:retry_timeout, integer()}]

  @doc """
  Appends sage with an cached transaction and function to compensate it's side effects.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.

      sage
      |> run_async(:a, tx_cb, cmp_cb)
      |> run_async(:b, tx_cb, cmp_cb, after: :a)
      |> run_async(:e, tx_cb, cmp_cb)
      |> run_async(:c, tx_cb, cmp_cb, after: [:b, :e])
  """
  @callback run_async(sage :: Sage.t(), apply :: Sage.transaction(), rollback :: Sage.compensation(), opts :: Keyword.t) :: Sage.t()

  @doc """
  Appends sage with an cached transaction and function to compensate it's side effects.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.
  """
  @callback run_cached(sage :: Sage.t(), apply :: Sage.transaction(), rollback :: Sage.compensation(), opts :: cache_opts()) :: Sage.t()

  @doc """
  Appends sage with an asynchronous cached transaction and function to compensate it's side effects.

  Next non-asynchronous operation will await for this function return.

  Cache is stored by calling a `Sage.CacheAdapter` implementation.
  """
  @callback run_async_cached(sage :: Sage.t(), apply :: Sage.transaction(), rollback :: Sage.compensation(), opts :: cache_opts()) :: Sage.t()

  @doc """
  Appends sage with an checkpoint at which forward retries should occur.

  Internally this is the same as using:

      |> run(fn _ -> {:ok, :noop}, fn state -> if adapter.retry?(state, retry_opts) do {:retry, state} else {:ok, state} end)

  TODO: Also can be used as point of synchronization, eg.
    - to persist temporary state in a DB to idempotently retry it;
    - to ack previous execution stage to MQ and create a new one.

  TODO: Rename to retry?
  """
  @callback checkpoint(sage :: Sage.t(), retry_opts :: retry_opts()) :: Sage.t()


  @doc """
  Register persistent adapter and idempotency key generator to make it possible to re-run same requests
  idempotently (by either replying with old success response or continuing from the latest failed Sage transaction).
  """
  @callback with_idempotency(sage :: Sage.t(), adapter :: module()) :: Sage.t()

  @doc """
  Concurrently run transaction after it's dependencies.

  Would allow to build a dependency tree and run everything with maximum concurrency.
  """
  @callback run_async_after(sage :: Sage.t(), [after_name :: Sage.name()], apply :: Sage.transaction(), rollback :: Sage.compensation()) :: Sage.t()
end
