defmodule Sage.TestCompensationErrorHandler do
  @moduledoc false
  @behaviour Sage.CompensationErrorHandler

  def handle_error({:exception, %Sage.MalformedCompensationReturnError{}, _stacktrace}, compensations_to_run, opts) do
    compensations_to_run
    |> List.delete_at(0)
    |> Enum.map(fn {name, compensation, effect_to_compensate} when is_function(compensation, 3) ->
      apply(Sage.Fixtures.not_strict_compensation(), [effect_to_compensate, {name, :exception}, opts])
    end)

    {:error, :failed_to_compensate_errors}
  end

  def handle_error(_error, compensations_to_run, opts) do
    Enum.map(compensations_to_run, fn {name, compensation, effect_to_compensate} when is_function(compensation, 3) ->
      apply(Sage.Fixtures.not_strict_compensation(), [effect_to_compensate, {name, :exception}, opts])
    end)

    {:error, :failed_to_compensate_errors}
  end
end
