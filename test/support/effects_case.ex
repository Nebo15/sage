defmodule Sage.EffectsCase do
  @moduledoc false
  use ExUnit.CaseTemplate
  alias Sage.EffectsAgent

  using do
    quote do
      import Sage.EffectsCase
      import Sage.Fixtures
      import Sage
    end
  end

  def assert_no_effects do
    effects = EffectsAgent.list_effects()
    assert effects == []
  end

  def assert_effect(effect) do
    effects = EffectsAgent.list_effects()
    assert effect in effects
  end

  def refute_effect(effect) do
    effects = EffectsAgent.list_effects()
    refute effect in effects
  end

  def assert_effects(expected_effects) do
    effects = EffectsAgent.list_effects()
    assert Enum.reverse(effects) == expected_effects
  end

  def final_hook_with_assertion(asserted_status, asserted_opts \\ nil) do
    test_pid = self()
    ref = make_ref()

    hook = &send(test_pid, {:finally, ref, &1, &2})

    assert_fun = fn ->
      assert_receive {:finally, ^ref, status, opts}, 500
      assert status == asserted_status
      if asserted_opts, do: assert(opts == asserted_opts)
    end

    {hook, assert_fun}
  end
end
