defmodule Sage.CompensationErrorHandler do
  @moduledoc """
  This module provides behaviour for compensation error handling.

  For more information see "Critical Error Handling" in Sage module doc.
  """

  @type error ::
          {:exception, exception :: Exception.t(), stacktrace :: Exception.stacktrace()}
          | {:exit, reason :: any()}
          | {:throw, error :: any()}

  @type compensations_to_run ::
          {name :: Sage.name(), compensation :: Sage.compensation(), effect_to_compensate :: any()}

  @doc """
  Handler for critical errors for compensation execution.

  It should return only `{:error, reason}` which would be returned by the Sage itself.
  """
  @callback handle_error(error :: error(), compensations_to_run :: compensations_to_run(), opts :: any()) ::
              {:error, reason :: any()}
end
