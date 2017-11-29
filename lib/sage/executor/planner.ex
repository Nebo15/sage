defmodule Sage.Executor.Planner do
  @moduledoc """
  This module is the one responsible for planning asynchronous transactions dependencies
  and making sure their dependency graph converges.
  """
  import Sage.ExecutorPlannerError

  def plan_execution(%Sage{} = sage) do
    {plan, _stage_names} =
      sage.stages
      |> Enum.reduce([], fn
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
      |> Enum.reduce({[], []}, fn
        {:stages_group, stages_group}, {stages, reachable_stage_names} ->
          sorted = topological_sort(stages_group, reachable_stage_names)
          {Enum.reverse(sorted) ++ stages, Enum.map(sorted, &elem(&1, 0)) ++ reachable_stage_names}

        {name, operation} = stage, {stages, reachable_stage_names} ->
          operation
          |> operation_deps()
          |> Enum.each(fn dep ->
            if dep == name do
              raise Sage.ExecutorPlannerError, dependency_on_itself_message(name)
            end

            if dep not in reachable_stage_names do
              raise Sage.ExecutorPlannerError, unreachable_dependency_message(name, dep)
            end
          end)

          {[stage] ++ stages, [elem(stage, 0)] ++ reachable_stage_names}
      end)

    Enum.reverse(plan)
  end

  defp async_stage?({_name, operation}) when elem(operation, 0) == :run_async, do: true
  defp async_stage?(_), do: false

  def topological_sort(stages, reachable_stage_names) do
    graph = :digraph.new()
    graph_names = Enum.map(stages, &elem(&1, 0))

    try do
      Enum.each(stages, fn {name, _operation} ->
        :digraph.add_vertex(graph, name)
      end)

      Enum.each(stages, fn {name, operation} ->
        deps = operation_deps(operation)

        Enum.each(deps, fn dep ->
          cond do
            dep == name ->
              raise Sage.ExecutorPlannerError, dependency_on_itself_message(name)

            dep in reachable_stage_names ->
              :noop

            dep in graph_names ->
              :digraph.add_edge(graph, dep, name)

            dep not in reachable_stage_names ->
              raise Sage.ExecutorPlannerError, unreachable_dependency_message(name, dep)
          end
        end)
      end)

      if ordered_stages = :digraph_utils.topsort(graph) do
        Enum.map(ordered_stages, fn name ->
          List.keyfind(stages, name, 0)
        end)
      else
        raise Sage.ExecutorPlannerError, can_not_converge_message()
      end
    after
      :digraph.delete(graph)
    end
  end

  defp operation_deps({_type, _tx, _cmp, opts}), do: opts |> Keyword.get(:after, []) |> List.wrap()
end
