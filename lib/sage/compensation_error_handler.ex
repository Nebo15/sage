defmodule Sage.CompensationErrorHandler do
  @moduledoc """
  This module provides behaviour for compensation error handling.

  Few solutions you might want to try:

  - Send notification to a Slack channel about need of manual resolution;
  - Retry compensation;
  - Spin off a new supervised process that would retry compensation and return an error in the Sage.
  (Useful when you have connection issues that would be resolved at some point in future.)

  For more information see "Critical Error Handling" in Sage module doc.
  """

  @type error ::
          {:exception, exception :: Exception.t(), stacktrace :: Exception.stacktrace()}
          | {:exit, reason :: any()}
          | {:throw, error :: any()}

  @type compensations_to_run ::
          {name :: Sage.stage_name(), compensation :: Sage.compensation(), effect_to_compensate :: any()}

  @doc """
  Handler for critical errors for compensation execution.

  It should return only `{:error, reason}` which would be returned by the Sage itself.
  """
  @callback handle_error(error :: error(), compensations_to_run :: compensations_to_run(), opts :: any()) ::
              {:error, reason :: any()}
end
