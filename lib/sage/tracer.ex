defmodule Sage.Tracer do
  @moduledoc """
    Hook which can executed before and after transaction or compensation.

  Function return is ignored. All exit's and raises are rescued and logged.

  ## Hooks State

  All hooks share their state, which by default contains options passed to `execute/2` function.
  Altering this state won't affect Sage execution in any way, changes would be visible only to other
  before and after hooks.
  """

  @callback handle_event(name :: Sage.name(), state :: any()) :: no_return
end
