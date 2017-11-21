use Mix.Config
alias Dogma.Rule

config :dogma,
  rule_set: Dogma.RuleSet.All,
  exclude: [
    ~r(\Adeps/),
  ],
  override: [
    %Rule.LineLength{max_length: 120},
    %Rule.InfixOperatorPadding{enabled: false},
    %Rule.FunctionArity{max: 5}
  ]
