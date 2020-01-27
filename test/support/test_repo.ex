defmodule TestRepo do
  def transaction(fun, opts) do
    send(self(), {:transaction, fun, opts})
    {:ok, fun.()}
  end

  def rollback(error) do
    send(self(), {:rollback, error})
    {:error, error}
  end
end
