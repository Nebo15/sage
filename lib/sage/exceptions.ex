defmodule Sage.EmptyError do
  @moduledoc """
  Raised at runtime when empty sage is executed.
  """
  defexception [:message]

  @doc false
  def exception(_opts) do
    message = "trying to execute empty Sage is not allowed"
    %__MODULE__{message: message}
  end
end

defmodule Sage.AsyncTransactionTimeoutError do
  @moduledoc """
  Raised at runtime when the asynchronous transaction timed out.
  """
  defexception [:name, :timeout]

  @impl true
  def message(%__MODULE__{name: name, timeout: timeout}) do
    """
    asynchronous transaction for operation #{name} has timed out,
    expected it to return within #{to_string(timeout)} microseconds
    """
  end
end

defmodule Sage.DuplicateStageError do
  @moduledoc """
  Raised at runtime when operation with duplicated name is added to Sage.
  """
  defexception [:message]

  @impl true
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

defmodule Sage.DuplicateTracerError do
  @moduledoc """
  Raised at runtime when a duplicated tracer is added to Sage.
  """
  defexception [:message]

  @impl true
  def exception(opts) do
    sage = Keyword.fetch!(opts, :sage)
    module = Keyword.fetch!(opts, :module)

    message = """
    #{inspect(module)} is already defined as tracer for Sage:

      #{inspect(sage)}
    """

    %__MODULE__{message: message}
  end
end

defmodule Sage.DuplicateFinalHookError do
  @moduledoc """
  Raised at runtime when duplicated final hook is added to Sage.
  """
  defexception [:message]

  @impl true
  def exception(opts) do
    sage = Keyword.fetch!(opts, :sage)
    callback = Keyword.fetch!(opts, :hook)

    message = """
    #{format_callback(callback)} is already defined as final hook for Sage:

      #{inspect(sage)}
    """

    %__MODULE__{message: message}
  end

  defp format_callback({m, f, a}), do: "#{inspect(m)}.#{to_string(f)}/#{to_string(length(a) + 2)}"
  defp format_callback(cb), do: inspect(cb)
end

defmodule Sage.MalformedTransactionReturnError do
  @moduledoc """
  Raised at runtime when the transaction or operation has an malformed return.
  """
  defexception [:stage, :transaction, :return]

  @impl true
  def message(%__MODULE__{stage: stage, transaction: transaction, return: return}) do
    """
    expected transaction #{inspect(transaction)} for stage #{inspect(stage)} to return
    {:ok, effect}, {:error, reason} or {:abort, reason}, got:

      #{inspect(return)}
    """
  end
end

defmodule Sage.MalformedCompensationReturnError do
  @moduledoc """
  Raised at runtime when the compensation or operation has an malformed return.
  """
  defexception [:stage, :compensation, :return]

  @impl true
  def message(%__MODULE__{stage: stage, compensation: compensation, return: return}) do
    """
    expected compensation #{inspect(compensation)} for stage #{inspect(stage)} to return
    :ok, :abort, {:retry, retry_opts} or {:continue, effect}, got:

      #{inspect(return)}
    """
  end
end
