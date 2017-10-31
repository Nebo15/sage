# Sage

All guarantees that transaction steps (independent atomic actions) are completed or all failed steps are compensated.

system guarantees that either all the transactions m a saga are successfully completed or compensatmg transactions are run to amend a partial execution

This is a library for Elixir that implements the [Saga][saga-paper] pattern for
error recovery/cleanup in distributed transactions. The saga pattern describes
two call flows, a forward flow that represents progress, and an opposite
rollback flow which represents recovery and cleanup activities.

## Goals

- Provide better way to do apply and rollback for a multiple-step actions.
- Provide adapters for Ecto.Multi and HTTP libs.
- Warn when running non-idempotent operations without rollback.
- Allow to run some operations concurrently.
- Allow to manage retry/timeout policies.

## Use Cases

- Updating state in two different databases.
- Updating state of a central RDBM's along with state updates issued by a HTTP calls.
- Updating remote state over multiple upstream API's. (Complex requests logic -> create customer, create subscription, charge subscription)


Sage allows you to use adapter to simplify integration with database and HTTP clients.

# RFC's

### Side effects

One side effect per do or allow do's to return `{:ok, result, [side_effects]}` tuple?

### Idempotency

Sage.new()
|> Sage.with_idempotency(enabled? \\ true) // or with_persistency()
|> Sage.do(..) # Ok, result is written to a persistent storage
|> Sage.do(..) # Ok, result is written
|> Sage.do(..) # Error, probably nothing to rollback and retry of this sage would continue from this step (state is fetched from DB)
|> Sage.do(..)

### HTTP lib on top of Sage

See https://gist.github.com/michalmuskala/5cee518b918aa5a441e757efca965d22.

HTTP client can extend Saga's interface and we will be able to:
- suggest the way to compensate request;
- require compensation for non-idempotent operations;
- reduce amount of code that needs to be written for complex API interactions.

#### Type checking

We can leverage dializer?

#### Tests

Integration with property testing would make possible to test your transaction functions and almost automatically make sure Saga is correct for most common scenarios.

### Ecto.Multi replacement & Database integrations & Runnin saga in Repo.transaction(...)

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `sage` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:sage, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/sage](https://hexdocs.pm/sage).


# License

See [LICENSE.md](LICENSE.md).

# Credits

Parts of the code and implementation ideas are taken from [`Ecto.Multi`](https://github.com/elixir-ecto/ecto/blob/master/lib/ecto/multi.ex) module originally implemented by @michalmuskala and [`gisla`](https://github.com/mrallen1/gisla) by @mrallen1 which implements Sagas for Erlang.

Sagas idea have origins from [this whitepaper](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) from 80's.
