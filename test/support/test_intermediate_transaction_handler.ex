defmodule TestIntermediateTransactionHandler do
  def intermediate_transaction_handler(_effects, _args, previous_stage_name, something_else) do
    {:ok, {previous_stage_name, something_else}}
  end
end
