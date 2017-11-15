defmodule Sage.TestTracer do
  def handle_event(name, state) do
    IO.inspect({name, state})
  end
end
