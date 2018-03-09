defmodule SageTest do
  use Sage.EffectsCase, async: true
  doctest Sage

  describe "with_compensation_error_handler/2" do
    test "registers on_compensation_error hook" do
      sage = new()
      assert sage.on_compensation_error == :raise
      sage = with_compensation_error_handler(sage, Sage.TestCompensationErrorHandler)
      assert sage.on_compensation_error == Sage.TestCompensationErrorHandler
    end
  end

  describe "with_tracer/2" do
    test "registers tracing hook" do
      sage = new()
      assert MapSet.equal?(sage.tracers, MapSet.new())
      sage = with_tracer(sage, Sage.TestTracer)
      assert MapSet.equal?(sage.tracers, MapSet.new([Sage.TestTracer]))
    end

    test "raises on duplicate tracers" do
      message = ~r"Sage.TestTracer is already defined as tracer for Sage:"

      assert_raise Sage.DuplicateTracerError, message, fn ->
        new()
        |> with_tracer(Sage.TestTracer)
        |> with_tracer(Sage.TestTracer)
      end
    end
  end

  describe "finally/2" do
    test "registers tracing hook with anonymous function" do
      sage = new()
      assert MapSet.equal?(sage.final_hooks, MapSet.new())
      cb = fn _, _ -> :ok end
      sage = finally(sage, cb)
      assert MapSet.equal?(sage.final_hooks, MapSet.new([cb]))
    end

    test "registers tracing hook with mfa tuple" do
      sage = new()
      assert MapSet.equal?(sage.final_hooks, MapSet.new())
      cb = {__MODULE__, :dummy_final_cb, [:ok]}
      sage = finally(sage, cb)
      assert MapSet.equal?(sage.final_hooks, MapSet.new([cb]))
    end

    test "raises on duplicate mfa hook" do
      message = ~r"SageTest.dummy_final_cb/3 is already defined as final hook for Sage:"

      assert_raise Sage.DuplicateFinalHookError, message, fn ->
        new()
        |> finally({__MODULE__, :dummy_final_cb, [:ok]})
        |> finally({__MODULE__, :dummy_final_cb, [:ok]})
      end
    end

    test "raises on duplicate callback" do
      cb = fn _, _ -> :ok end

      message = ~r"#{inspect(cb)} is already defined as final hook for Sage:"

      assert_raise Sage.DuplicateFinalHookError, message, fn ->
        new()
        |> finally(cb)
        |> finally(cb)
      end
    end
  end

  describe "to_function/4" do
    test "wraps sage in function" do
      sage = new() |> run(:step1, fn %{}, foo: "bar" -> {:ok, :t1} end)
      fun = to_function(sage, foo: "bar")
      assert is_function(fun, 0)
      assert fun.() == {:ok, :t1, %{step1: :t1}}
    end
  end

  describe "transaction/2" do
    test "executes the sage" do
      sage = new() |> run(:step1, fn %{}, [] -> {:ok, :t1} end)
      assert transaction(sage, TestRepo) == {:ok, :t1, %{step1: :t1}}
      assert_receive {:transaction, _fun}
    end

    test "executes the sage with opts" do
      sage = new() |> run(:step1, fn %{}, foo: "bar" -> {:ok, :t1} end)
      assert transaction(sage, TestRepo, foo: "bar") == {:ok, :t1, %{step1: :t1}}
      assert_receive {:transaction, _fun}
    end

    test "rollbacks transaction on errors" do
      sage = new() |> run(:step1, fn %{}, [] -> {:error, :foo_bar} end)
      assert transaction(sage, TestRepo) == {:error, :foo_bar}
      assert_receive {:transaction, _fun}
    end

    test "raises when there are no stages to execute" do
      sage = new()

      assert_raise Sage.EmptyError, "trying to execute empty Sage is not allowed", fn ->
        transaction(sage, TestRepo)
      end

      assert_receive {:transaction, _fun}
    end
  end

  describe "execute/4" do
    test "executes the sage" do
      sage = new() |> run(:step1, fn %{}, [] -> {:ok, :t1} end)
      assert execute(sage) == {:ok, :t1, %{step1: :t1}}
    end

    test "executes the sage with opts" do
      sage = new() |> run(:step1, fn %{}, foo: "bar" -> {:ok, :t1} end)
      assert execute(sage, foo: "bar") == {:ok, :t1, %{step1: :t1}}
    end

    test "raises when there are no stages to execute" do
      sage = new()

      assert_raise Sage.EmptyError, "trying to execute empty Sage is not allowed", fn ->
        execute(sage)
      end
    end
  end

  describe "run/3" do
    test "adds operation via anonymous function to a sage" do
      tx = transaction(:t1)
      %Sage{stages: stages, stage_names: names} = run(new(), :step1, tx)
      assert {:step1, {:run, tx, :noop, []}} in stages
      assert MapSet.member?(names, :step1)
    end

    test "adds operation via mfa tuple to a sage" do
      tx = {__MODULE__, :dummy_transaction_for_mfa, []}
      %Sage{stages: stages, stage_names: names} = run(new(), :step1, tx)
      assert {:step1, {:run, tx, :noop, []}} in stages
      assert MapSet.member?(names, :step1)
    end
  end

  describe "run/4" do
    test "adds compensation via anonymous function to a sage" do
      tx = transaction(:t1)
      cmp = compensation()
      %Sage{stages: stages, stage_names: names} = run(new(), :step1, tx, cmp)
      assert {:step1, {:run, tx, cmp, []}} in stages
      assert MapSet.member?(names, :step1)
    end

    test "adds compensation via mfa tuple to a sage" do
      tx = transaction(:t1)
      cmp = {__MODULE__, :dummy_compensation_for_mfa, []}
      %Sage{stages: stages, stage_names: names} = run(new(), :step1, tx, cmp)
      assert {:step1, {:run, tx, cmp, []}} in stages
      assert MapSet.member?(names, :step1)
    end

    test "allows to user :noop for compensation" do
      tx = transaction(:t1)
      %Sage{stages: stages, stage_names: names} = run(new(), :step1, tx, :noop)
      assert {:step1, {:run, tx, :noop, []}} in stages
      assert MapSet.member?(names, :step1)
    end

    test "raises when on duplicate names" do
      message = ~r":step1 is already a member of the Sage:"

      assert_raise Sage.DuplicateStageError, message, fn ->
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step1, transaction(:t2), compensation())
      end
    end
  end

  describe "run_async/4" do
    test "adds compensation via anonymous function to a sage" do
      tx = transaction(:t1)
      cmp = compensation()
      %Sage{stages: stages, stage_names: names} = run_async(new(), :step1, tx, cmp, timeout: 5_000)
      assert {:step1, {:run_async, tx, cmp, [timeout: 5_000]}} in stages
      assert MapSet.member?(names, :step1)
    end

    test "adds compensation via mfa tuple to a sage" do
      tx = transaction(:t1)
      cmp = {__MODULE__, :dummy_compensation_for_mfa, []}
      %Sage{stages: stages, stage_names: names} = run_async(new(), :step1, tx, cmp, timeout: 5_000)
      assert {:step1, {:run_async, tx, cmp, [timeout: 5_000]}} in stages
      assert MapSet.member?(names, :step1)
    end
  end

  def dummy_transaction_for_mfa(_effects_so_far, _opts), do: raise("Not implemented")
  def dummy_compensation_for_mfa(_effect_to_compensate, _name_and_reason, _opts), do: raise("Not implemented")
  def dummy_final_cb(_status, _opts, _return), do: raise("Not implemented")
end
