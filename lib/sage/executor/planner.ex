defmodule Sage.Executor.Planner do
  @moduledoc """
  This module is the one responsible for planning asynchronous transactions dependencies
  and making sure their dependency graph converges.
  """
  import Sage.Executor.PlannerError

  def plan_execution(%Sage{} = sage) do
    sage.stages
    |> group_async_stages()
    |> topological_sort_groups()
    |> Enum.reverse()
  end

  # Groups multiple adjustment stages
  defp group_async_stages(stages) do
    Enum.reduce(stages, [], fn
      stage, [] ->
        [stage]

      stage, [{:stages_group, prev_stages} | rest_stages] = acc ->
        if async_stage?(stage) do
          [{:stages_group, [stage] ++ prev_stages}] ++ rest_stages
        else
          [stage] ++ acc
        end

      stage, [prev_stage | rest_stages] = acc ->
        if async_stage?(stage) && async_stage?(prev_stage) do
          [{:stages_group, [prev_stage, stage]}] ++ rest_stages
        else
          [stage] ++ acc
        end
    end)
  end

  defp async_stage?({_name, operation}) when elem(operation, 0) == :run_async, do: true
  defp async_stage?(_), do: false

  defp topological_sort_groups(stages) do
    {stages, _stage_names} =
      Enum.reduce(stages, {[], []}, fn
        {:stages_group, stages_group}, {stages, reachable_stage_names} ->
          sorted = topological_sort(stages_group, reachable_stage_names)
          {Enum.reverse(sorted) ++ stages, Enum.map(sorted, &elem(&1, 0)) ++ reachable_stage_names}

        {name, operation} = stage, {stages, reachable_stage_names} ->
          operation
          |> operation_deps()
          |> Enum.each(&raise_on_invalid_dependency!(name, &1, reachable_stage_names))

          {[stage] ++ stages, [name] ++ reachable_stage_names}
      end)

    stages
  end

  def topological_sort(stages, reachable_stage_names) do
    graph = :digraph.new()

    try do
      Enum.each(stages, fn {name, _operation} ->
        :digraph.add_vertex(graph, name)
      end)

      Enum.each(stages, fn {name, operation} ->
        deps = operation_deps(operation)

        Enum.each(deps, fn dep ->
          if dep != name and List.keymember?(stages, dep, 0) do
            :digraph.add_edge(graph, dep, name)
          else
            raise_on_invalid_dependency!(name, dep, reachable_stage_names)
          end
        end)
      end)

      if ordered_stages = :digraph_utils.topsort(graph) do
        Enum.map(ordered_stages, fn name ->
          List.keyfind(stages, name, 0)
        end)
      else
        raise Sage.Executor.PlannerError, can_not_converge_message()
      end
    after
      :digraph.delete(graph)
    end
  end

  defp operation_deps({_type, _tx, _cmp, opts}), do: opts |> Keyword.get(:after, []) |> List.wrap()

  defp raise_on_invalid_dependency!(stage_name, stage_name, _reachable_stage_names),
    do: raise Sage.Executor.PlannerError, dependency_on_itself_message(stage_name)
  defp raise_on_invalid_dependency!(stage_name, dependency_name, reachable_stage_names) do
    if dependency_name not in reachable_stage_names do
      raise Sage.Executor.PlannerError, unreachable_dependency_message(stage_name, dependency_name)
    end
  end
end
