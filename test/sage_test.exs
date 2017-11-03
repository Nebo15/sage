defmodule CountingAgent do
  def start_link(initial) do
    Agent.start_link fn -> initial end
  end

  def inc(agent) do
    Agent.update(agent, fn counter -> counter + 1 end)
  end

  def dec(agent) do
    Agent.update(agent, fn counter -> counter - 1 end)
  end

  def get(agent) do
    Agent.get(agent, fn counter -> counter end)
  end
end

defmodule SideEffectAgent do
  def start_link do
    Agent.start_link fn -> [] end
  end

  def create_side_effect(agent, effect) do
    Agent.update(agent, fn effects ->
      if effect not in effects do
        [effect | effects]
      else
        raise "Trying to re-apply stale effect"
      end
    end)
  end

  def delete_side_effect(agent, effect) do
    Agent.update(agent, fn effects ->
      if effect in effects do
        List.delete(effects, effect)
      else
        raise "Effect not found"
      end
    end)
  end

  def side_effects(agent) do
    Agent.get(agent, fn list -> Enum.reverse(list) end)
  end
end

defmodule SageTest do
  use ExUnit.Case
  import Sage
  doctest Sage

  test "applies transactions" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step3, &tx_ok(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> finally(fn :ok -> :ok end)
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == [:t1, :t2, :t3]
    assert result == {:ok, :t3, %{step1: :t1, step2: :t2, step3: :t3}}
  end

  test "compensates errors on last step" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step3, &tx_err(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> finally(fn :error -> :results_are_ignored_mmkay end)
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == []
    assert result == {:error, :t3}
  end

  test "compensates errors in the middle" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step2, &tx_err(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step3, &tx_ok(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == []
    assert result == {:error, :t2}
  end

  test "reties transactions" do
    {:ok, agent} = SideEffectAgent.start_link()
    {:ok, counter} = CountingAgent.start_link(3)

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_retry(agent, &1, &2, &3))
      |> run(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step3, &tx_err_n_times(agent, counter, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == [:t1, :t2, :t3]
    assert result == {:ok, :t3, %{step1: :t1, step2: :t2, step3: :t3}}
  end

  test "reties count" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_retry(agent, &1, &2, &3, 3))
      |> run(:step3, &tx_err(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == []
    assert result == {:error, :t3}
  end

  test "ignores retries on transactions abort" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_retry(agent, &1, &2, &3))
      |> run(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step3, &tx_err(agent, :t3, &1, &2), &cmp_abort(agent, &1, &2, &3))
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == []
    assert result == {:error, :t3}
  end

  test "circuit breaker" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step2, &tx_err(agent, :t2, &1, &2), &cmp_continue(agent, &1, &2, &3))
      |> run(:step3, &tx_ok(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == [:t1, :t3]
    assert result == {:ok, :t3, %{step1: :t1, step2: :fallback_return, step3: :t3}}
  end

  test "circuit breaker raises when other stage is failed" do
    {:ok, agent} = SideEffectAgent.start_link()

    assert_raise RuntimeError, "Circuit breaking is only allowed for continuing compensated transaction", fn ->
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_continue(agent, &1, &2, &3))
      |> run(:step3, &tx_err(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> execute([a: :b])
    end
  end

  test "async txs" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run_async(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run_async(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run(:step3, &tx_ok(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == [:t1, :t2, :t3]
    assert result == {:ok, :t3, %{step1: :t1, step2: :t2, step3: :t3}}
  end

  test "end with async txs" do
    {:ok, agent} = SideEffectAgent.start_link()

    result =
      new()
      |> run(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run_async(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> run_async(:step3, &tx_ok(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
      |> execute([a: :b])

    assert SideEffectAgent.side_effects(agent) == [:t1, :t2, :t3]
    assert result == {:ok, :t3, %{step1: :t1, step2: :t2, step3: :t3}}
  end

  describe "compensates errors async txs" do
    test "when error occurrences is after async operation" do
      {:ok, agent} = SideEffectAgent.start_link()

      result =
        new()
        |> run_async(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
        |> run_async(:step2, &tx_ok(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
        |> run(:step3, &tx_err(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
        |> execute([a: :b])

      assert SideEffectAgent.side_effects(agent) == []
      assert result == {:error, :t3}
    end

    test "when error occurrences is on of async operations" do
      {:ok, agent} = SideEffectAgent.start_link()

      result =
        new()
        |> run_async(:step1, &tx_ok(agent, :t1, &1, &2), &cmp_ok(agent, &1, &2, &3))
        |> run_async(:step2, &tx_err(agent, :t2, &1, &2), &cmp_ok(agent, &1, &2, &3))
        |> run_async(:step3, &tx_ok(agent, :t3, &1, &2), &cmp_ok(agent, &1, &2, &3))
        |> execute([a: :b])

      assert SideEffectAgent.side_effects(agent) == []
      assert result == {:error, :t2}
    end
  end

  def tx_ok(agent_pid, tid, effects_so_far, opts) do
    SideEffectAgent.create_side_effect(agent_pid, tid)
    IO.inspect {tid, effects_so_far, opts}, label: "TX: "
    {:ok, tid}
  end

  def tx_abort(agent_pid, tid, effects_so_far, opts) do
    SideEffectAgent.create_side_effect(agent_pid, tid)
    IO.inspect {tid, effects_so_far, opts}, label: "TX abort: "
    {:abort, tid}
  end

  def tx_err(agent_pid, tid, effects_so_far, opts) do
    SideEffectAgent.create_side_effect(agent_pid, tid)
    IO.inspect {tid, effects_so_far, opts}, label: "TX error: "
    {:error, tid}
  end

  def tx_err_n_times(agent_pid, counter_pid, tid, effects_so_far, opts) do
    if CountingAgent.get(counter_pid) > 0 do
      SideEffectAgent.create_side_effect(agent_pid, tid)
      IO.inspect {tid, effects_so_far, opts}, label: "TX error: "
      CountingAgent.dec(counter_pid)
      {:error, tid}
    else
      tx_ok(agent_pid, tid, effects_so_far, opts)
    end
  end

  def cmp_ok(agent_pid, effect_to_compensate, {name, reason}, opts) do
    SideEffectAgent.delete_side_effect(agent_pid, effect_to_compensate)
    IO.inspect {effect_to_compensate, {name, reason}, opts}, label: "CMP ok: "
    :ok
  end

  # I am compensated by transaction, let's retry with this data from my tx
  def cmp_retry(agent_pid, effect_to_compensate, {name, reason}, opts, limit \\ 3) do
    SideEffectAgent.delete_side_effect(agent_pid, effect_to_compensate)
    IO.inspect {effect_to_compensate, {name, reason}, opts}, label: "CMP retry: "
    {:retry, [retry_limit: limit]}
  end

  # I am compensated transaction and want to force backwards recovery on all steps
  def cmp_abort(agent_pid, effect_to_compensate, {name, reason}, opts) do
    SideEffectAgent.delete_side_effect(agent_pid, effect_to_compensate)
    IO.inspect {effect_to_compensate, {name, reason}, opts}, label: "CMP abort: "
    :abort
  end

  # I am the Circuit Breaker and I know how live wit this error
  def cmp_continue(agent_pid, effect_to_compensate, {name, reason}, opts) do
    SideEffectAgent.delete_side_effect(agent_pid, effect_to_compensate)
    IO.inspect {effect_to_compensate, {name, reason}, opts}, label: "CMP cont: "
    {:continue, :fallback_return}
  end
end
