defmodule TestRepo do
  def transaction(fun, opts) do
    send(self(), {:transaction, fun, opts})

    case fun.() do
      {:error, reason} -> {:error, reason}
      result -> {:ok, result}
    end
  end

  def rollback(error) do
    send(self(), {:rollback, error})
    {:error, error}
  end
end
