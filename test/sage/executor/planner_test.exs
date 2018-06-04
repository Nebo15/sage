defmodule Sage.Executor.PlannerTest do
  use Sage.EffectsCase
  import Sage.Executor.Planner

  describe "plan_execution/1" do
     test "ignores synchronous operations" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction(:t2), compensation())
        |> run(:step3, transaction(:t3), compensation())

      assert sage |> plan_execution() |> names() == [:step1, :step2, :step3]
    end

    test "ignores single asynchronous operations" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t2), compensation())
        |> run(:step3, transaction(:t3), compensation())

      assert sage |> plan_execution() |> names() == [:step1, :step2, :step3]
    end

    test "allows asynchronous operations to depend on reachable synchronous operations" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t2), compensation(), after: :step1)
        |> run(:step3, transaction(:t3), compensation())

      assert sage |> plan_execution() |> names() == [:step1, :step2, :step3]
    end

    test "raises on unreachable synchronous operations" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t2), compensation(), after: :step3)
        |> run(:step3, transaction(:t3), compensation())

      assert_raise Sage.Executor.PlannerError, "Unreachable dependency step3 for stage step2", fn ->
        plan_execution(sage)
      end
    end

    test "raises on unreachable asynchronous operations" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t2), compensation(), after: :step4)
        |> run(:step3, transaction(:t3), compensation())
        |> run_async(:step4, transaction(:t4), compensation())

      assert_raise Sage.Executor.PlannerError, "Unreachable dependency step4 for stage step2", fn ->
        plan_execution(sage)
      end

      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t2), compensation(), after: :step5)
        |> run_async(:step3, transaction(:t3), compensation())
        |> run(:step4, transaction(:t4), compensation())
        |> run_async(:step5, transaction(:t5), compensation())

      assert_raise Sage.Executor.PlannerError, "Unreachable dependency step5 for stage step2", fn ->
        plan_execution(sage)
      end
    end

    test "raises on dependency on undefined stages" do
      sage = run_async(new(), :step1, transaction(:t1), compensation(), after: :undefined)
      assert_raise Sage.Executor.PlannerError, "Unreachable dependency undefined for stage step1", fn ->
        plan_execution(sage)
      end
    end

    test "raises on stage that depends on itself" do
      sage = run_async(new(), :step1, transaction(:t1), compensation(), after: :step1)
      assert_raise Sage.Executor.PlannerError, "Stage step1 lists itself as a dependency", fn ->
        plan_execution(sage)
      end

      sage =
        new()
        |> run_async(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t1), compensation(), after: :step2)

      assert_raise Sage.Executor.PlannerError, "Stage step2 lists itself as a dependency", fn ->
        plan_execution(sage)
      end
    end

    test "raises on circular dependencies" do
      sage =
        new()
        |> run_async(:step1, transaction(:t1), compensation(), after: :step2)
        |> run_async(:step2, transaction(:t1), compensation(), after: :step1)

      message = "Could not sort dependencies. There are cycles in the dependency graph"
      assert_raise Sage.Executor.PlannerError, message, fn ->
        plan_execution(sage)
      end

      sage =
        new()
        |> run_async(:step1, transaction(:t1), compensation(), after: :step3)
        |> run_async(:step2, transaction(:t1), compensation(), after: :step1)
        |> run_async(:step3, transaction(:t1), compensation(), after: :step2)

      message = "Could not sort dependencies. There are cycles in the dependency graph"
      assert_raise Sage.Executor.PlannerError, message, fn ->
        plan_execution(sage)
      end
    end

    test "does not reorder asynchronous stages without dependencies" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t2), compensation())
        |> run_async(:step3, transaction(:t3), compensation())
        |> run(:step4, transaction(:t4), compensation())

      assert sage |> plan_execution() |> names() == [:step1, :step2, :step3, :step4]
    end

    test "orders asynchronous stages by their dependencies topology" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run_async(:step2, transaction(:t2), compensation(), after: :step4)
        |> run_async(:step3, transaction(:t3), compensation(), after: :step1)
        |> run_async(:step4, transaction(:t4), compensation())
        |> run_async(:step5, transaction(:t5), compensation(), after: :step3)
        |> run_async(:step6, transaction(:t6), compensation(), after: [:step3, :step1])
        |> run_async(:step7, transaction(:t7), compensation())

      expected_plan = [:step1, :step4, :step7, :step3, :step2, :step5, :step6]
      assert sage |> plan_execution() |> names() == expected_plan
    end
  end

  defp names(stages), do: Enum.map(stages, &elem(&1, 0))
end
