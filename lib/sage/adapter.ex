defmodule Sage.Adapter do
  @moduledoc """
  This module provides behaviour for Sage executors.
  """

  @doc """
  Executes a Sage.

  For more details see `Sage.execute/2`.
  """
  @callback execute(sage :: Sage.t(), opts :: any()) ::
              {:ok, result :: any(), effects :: Sage.effects()} | {:error, any()}
end
