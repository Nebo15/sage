defmodule Sage.InspectTest do
  use ExUnit.Case, async: true
  import Sage
  import Sage.Fixtures

  test "outputs operations" do
    tx = transaction(:t)
    cmp = compensation()

    sage =
      new()
      |> run(:step1, tx)
      |> run(:step2, tx, cmp)
      |> run_async(:step3, tx, cmp, timeout: 5_000)
      |> run(:step4, {__MODULE__, :transaction, [:foo, :bar]}, cmp)
      |> run(:step5, {__MODULE__, :transaction, []}, cmp)
      |> run(:step6, tx, {__MODULE__, :compensation, [:foo, :bar]})
      |> run(:step7, tx, {__MODULE__, :compensation, []})

    string =
      """
      step1: -> #{inspect(tx)}, \
      step2: -> #{inspect(tx)}
               <- #{inspect(cmp)}, \
      step3: -> #{inspect(tx)} (async) [timeout: 5000]
               <- #{inspect(cmp)}, \
      step4: -> Sage.InspectTest.transaction(effects_so_far, opts, :foo, :bar)
               <- #{inspect(cmp)}, \
      step5: -> Sage.InspectTest.transaction/2
               <- #{inspect(cmp)}, \
      step6: -> #{inspect(tx)}
               <- Sage.InspectTest.compensation(effect_to_compensate, name_and_reason, opts, :foo, :bar), \
      step7: -> #{inspect(tx)}
               <- Sage.InspectTest.compensation/3
      """
      |> String.trim()

    assert i(sage) == string
  end

  test "outputs final hooks" do
    fun = fn _, _ -> :ok end

    sage =
      new()
      |> finally(fun)
      |> finally({__MODULE__, :do_send, [:a, :b, :c]})
      |> finally({__MODULE__, :do_send, []})

    string =
      """
      finally: #{inspect(fun)}, \
      finally: Sage.InspectTest.do_send/2, \
      finally: Sage.InspectTest.do_send(name, state, :a, :b, :c)
      """
      |> String.trim()

    assert i(sage) == string
  end

  test "outputs compensation error handler" do
    sage =
      new()
      |> with_compensation_error_handler(Sage.TestCompensationErrorHandler)

    assert inspect(sage) == "#Sage(with Sage.TestCompensationErrorHandler)<>"
  end

  def i(%{on_compensation_error: :raise} = sage) do
    assert "#Sage<" <> rest = inspect(sage, limit: 50, printable_limit: 4096, width: 80)
    size = byte_size(rest)
    assert ">" = :binary.part(rest, size - 1, 1)
    :binary.part(rest, 0, size - 1)
  end
end
