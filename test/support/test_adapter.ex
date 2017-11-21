defmodule Sage.TestAdapter do
  @moduledoc false

  def execute(sage, opts) do
    {:ok, :executed, %{sage: sage, opts: opts}}
  end
end
