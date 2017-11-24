defmodule Sage.EffectsAgent do
  @moduledoc false
  def start_link do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def push_effect!(effect, pid \\ :self) do
    pid = resolve_pid(pid)

    Agent.get_and_update(__MODULE__, fn state ->
      effects = Map.get(state, pid, [])

      if effect in effects do
        message = """
        Effect #{effect} already exists for PID #{inspect(pid)}.
        Effects dump:

          #{Enum.join(effects, " ")}
        """

        {{:error, message}, state}
      else
        # IO.inspect [effect | effects], label: "push #{to_string(effect)} pid #{inspect(pid)}"
        {:ok, Map.put(state, pid, [effect | effects])}
      end
    end)
    |> return_or_raise()
  end

  def pop_effect!(effect, pid \\ :self) do
    pid = resolve_pid(pid)

    Agent.get_and_update(__MODULE__, fn state ->
      effects = Map.get(state, pid, [])
      {last_effect, effects_tail} = List.pop_at(effects, 0)

      cond do
        effect not in effects ->
          message = """
          Effect #{effect} does not exists for PID #{inspect(pid)}.
          Effects dump:

            #{Enum.join(effects, " ")}
          """

          {{:error, message}, state}

        last_effect == effect ->
          # IO.inspect effects_tail, label: "pop #{to_string(effect)} for pid #{inspect(pid)}"
          {:ok, Map.put(state, pid, effects_tail)}

        effect in effects ->
          message = """
          Effect #{effect} is deleted out of order of it's creation for PID #{inspect(pid)}.
          Effects dump:

            #{Enum.join(effects, " ")}
          """

          {{:error, message}, state}
      end
    end)
    |> return_or_raise()
  end

  def delete_effect!(effect, pid \\ :self) do
    pid = resolve_pid(pid)

    Agent.get_and_update(__MODULE__, fn state ->
      effects = Map.get(state, pid, [])

      cond do
        effect not in effects ->
          message = """
          Effect #{effect} does not exists for PID #{inspect(pid)}.
          Effects dump:

            #{Enum.join(effects, " ")}
          """

          {{:error, message}, state}

        effect in effects ->
          effects = Enum.reject(effects, &(&1 == effect))
          # IO.inspect effects, label: "delete #{to_string(effect)} pid #{inspect(pid)}"
          {:ok, Map.put(state, pid, effects)}
      end
    end)
    |> return_or_raise()
  end

  def list_effects(pid \\ :self) do
    pid = resolve_pid(pid)

    Agent.get(__MODULE__, fn state ->
      Map.get(state, pid, [])
    end)
  end

  defp resolve_pid(:self), do: self()
  defp resolve_pid(pid), do: pid

  defp return_or_raise(:ok), do: :ok
  defp return_or_raise({:error, message}), do: raise(message)
end
