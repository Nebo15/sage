defmodule Sage.TestCompensationErrorHandler do
  def handle_error(a, b) do
    IO.inspect({a, b})
  end
end
