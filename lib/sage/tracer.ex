defmodule Sage.Tracer do
  @moduledoc """
  This module provides behaviour for Sage tracers.

  Tracing module is called before and after each transaction or compensation. For asynchronous operations
  this hook is triggered after awaiting for started transactions.

  ## Hooks State

  All hooks share their state, which by default contains options passed to `execute/2` function.
  This is useful if you want to persist timer of execution start and then persist it somewhere.

  Altering this state won't affect Sage execution in any way, changes would be visible only to
  other tracing calls.
  """

  @type action :: :start_transaction | :finish_transaction | :start_compensation | :finish_compensation

  @doc """
  Handler for Sage execution event.

  It receives name of Sage execution stage, type of event (see `t:action/0`)
  and state shared for all tracing calls (see "Hooks State" in module doc).

  Returns updated state.
  """
  @callback handle_event(name :: Sage.name(), action :: action(), state :: any()) :: any()
end
