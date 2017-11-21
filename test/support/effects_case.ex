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

  def assert_finally_fails(sage, asserted_opts \\ nil) do
    Sage.finally(sage, fn status, opts ->
      if asserted_opts, do: assert(opts == asserted_opts)
      assert status == :error
    end)
  end

  def assert_finally_succeeds(sage, asserted_opts \\ nil) do
    Sage.finally(sage, fn status, opts ->
      if asserted_opts, do: assert(opts == asserted_opts)
      assert status == :ok
    end)
  end
end
