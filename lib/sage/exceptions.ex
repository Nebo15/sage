defmodule Sage.UnexpectedCircuitBreakError do
  @moduledoc """
  Raised at runtime when the compensation tries to apply circuit breaker
  on transactions it's not responsible for.
  """
  defexception [:compensation_name, :failed_transaction_name]

  def message(%__MODULE__{compensation_name: compensation_name, failed_transaction_name: failed_transaction_name}) do
    """
    Compensation #{to_string(compensation_name)} tried to apply circuit
    breaker on a failure which occurred on transaction
    #{to_string(failed_transaction_name)} which it is not responsible for.

    If you trying to implement circuit breaker, always match for a
    failed operation name in compensating function:

    compensation =
      fn
        effect_to_compensate, {:my_step, _failure_reason}, _opts ->
          # 1. Compensate the side effect
          # 2. Continue with circuit breaker

        effect_to_compensate, {_not_my_step, _failure_reason}, _opts ->
          # Compensate the side effect
      end
    """
  end
end

defmodule Sage.AsyncTransactionTimeoutError do
  @moduledoc """
  Raised at runtime when the asynchronous transaction timed out.
  """
  defexception [:name, :timeout]

  def message(%__MODULE__{name: name, timeout: timeout}) do
    """
    asynchronous transaction for operation #{name} timed out,
    expected it to return within #{to_string(timeout)} microseconds
    """
  end
end

defmodule Sage.DuplicateOperationError do
  @moduledoc """
  Raised at runtime when operation with duplicated name is added to Sage.
  """
  defexception [:message]

  def exception(opts) do
    sage = Keyword.fetch!(opts, :sage)
    name = Keyword.fetch!(opts, :name)

    message = """
    #{inspect(name)} is already a member of the Sage:

      #{inspect(sage)}
    """

    %__MODULE__{message: message}
  end
end

defmodule Sage.MalformedTransactionReturnError do
  @moduledoc """
  Raised at runtime when the transaction or operation has an malformed return.
  """
  defexception [:transaction, :return]

  def message(%__MODULE__{transaction: transaction, return: return}) do
    """
    unexpected return from transaction #{inspect(transaction)},
    expected it to be {:ok, effect}, {:error, reason} or {:abort, reason}, got:

      #{inspect(return)}
    """
  end
end
