defmodule Sage.TestAdapter do
  def execute(sage, opts) do
    {:ok, :executed, %{sage: sage, opts: opts}}
  end
end
