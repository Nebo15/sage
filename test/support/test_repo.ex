defmodule TestRepo do
  def transaction(fun) do
    send(self(), {:transaction, fun})
    {:ok, fun.()}
  end

  def rollback(error) do
    send(self(), {:rollback, error})
    {:error, error}
  end
end
