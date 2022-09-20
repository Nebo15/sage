defmodule Sage.Executor.Retries do
  @moduledoc """
  This module implements retry logic with exponential back-off for compensations that want
  to retry transaction.
  """
  require Logger

  @doc """
  Returns `true` if transaction should be retried, `false` - otherwise.
  Optionally, return would be delayed with an exponential backoff based on `t:Sage.retry_opts/0`.

  Malformed retry options would be logged and ignored.
  """
  @spec retry_with_backoff?(attempt :: pos_integer(), opts :: Sage.retry_opts()) :: boolean
  def retry_with_backoff?(attempt, opts) do
    limit = Keyword.get(opts, :retry_limit)

    if is_integer(limit) && limit > attempt do
      base_backoff = Keyword.get(opts, :base_backoff)
      max_backoff = Keyword.get(opts, :max_backoff, 5_000)
      jitter_enabled? = Keyword.get(opts, :enable_jitter, true)

      backoff = get_backoff(attempt, base_backoff, max_backoff, jitter_enabled?)
      :ok = maybe_sleep(backoff)
      true
    else
      false
    end
  end

  @spec get_backoff(
          attempt :: pos_integer,
          base_backoff :: pos_integer() | nil,
          max_backoff :: pos_integer | nil,
          jitter_enabled :: boolean()
        ) :: non_neg_integer()
  @doc false
  # This function is public for testing purposes
  def get_backoff(_attempt, nil, _max_backoff, _jitter_enabled?) do
    0
  end

  def get_backoff(attempt, base_backoff, max_backoff, true)
      when is_integer(base_backoff) and base_backoff >= 1 and is_integer(max_backoff) and max_backoff >= 1 do
    random(calculate_backoff(attempt, base_backoff, max_backoff))
  end

  def get_backoff(attempt, base_backoff, max_backoff, _jitter_enabled?)
      when is_integer(base_backoff) and base_backoff >= 1 and is_integer(max_backoff) and max_backoff >= 1 do
    calculate_backoff(attempt, base_backoff, max_backoff)
  end

  def get_backoff(_attempt, base_backoff, max_backoff, _jitter_enabled?) do
    _ =
      Logger.error(
        "[Sage] Ignoring retry backoff options, expected base_backoff and max_backoff to be integer and >= 1, got: " <>
          "base_backoff: #{inspect(base_backoff)}, max_backoff: #{inspect(max_backoff)}"
      )

    0
  end

  defp calculate_backoff(attempt, base_backoff, max_backoff),
    do: min(max_backoff, trunc(:math.pow(base_backoff * 2, attempt)))

  defp random(n) when is_integer(n) and n > 0, do: :rand.uniform(n) - 1
  defp random(n) when is_integer(n), do: 0

  defp maybe_sleep(0), do: :ok
  defp maybe_sleep(backoff), do: :timer.sleep(backoff)
end
