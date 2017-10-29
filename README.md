# Sage

All guarantees that transaction steps (independent atomic actions) are completed or all failed steps are compensated.

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

## Terminology

# The database management
```
# system guarantees that either all the transactions m a saga are successfully completed or compensatmg transactions are run to amend a partial execution
transaction(..., compensate_transaction :: function)

```

### Direct usage

```elixir
Sage.new()
|> Sage.do(:create_user,
  fun _state, _opts -> HTTPAdapter.post("http://example.com/abc") end,
  fun %{create_user: created_user}, _opts ->
    HTTPAdapter.delete("http://example.com/abc/#{created_user[:user_id]")
  end
)
|> Sage.do(:add_to_subscribers, after: [:create_user],
  fun %{create_user: created_user}, %{list_id: list_id} ->
    HTTPAdapter.post("http://example.com/lists/#{list_id}/subscribers", created_user)
  end,
  fun %{add_to_subscribers: subscriber}, %{list_id: list_id} ->
    HTTPAdapter.delete("http://example.com/lists/#{list_id}/subscribers/#{subscriber[:subscribe_id]")
  end
)
|> Sage.do(:send_welcome_email, after: [:create_user],
  fun %{create_user: created_user}, _opts ->
    HTTPAdapter.post("http://example.com/send_email", %{to: created_user.email, body: "Hello there!"})
  end
)
|> Sage.execute(%{list_id: 123})
```

or

```elixir
defmodule MySage do
  def execute(opts) do
    Sage.new()
    |> Sage.do(:create_user, retries: 3, timeout: 5000)
    |> Sage.do(:add_to_subscribers)
    |> Sage.do(:send_welcome_email)
    |> Sage.execute(opts)
  end

  defp create_user(:forward, state, opts) do
    opts[:http_adapter].post("http://example.com/abc")
  end
  defp create_user(:rollback, state, opts)

  defp add_to_subscribers(:forward, state, opts)
  defp add_to_subscribers(:rollback, state, opts)

  defp send_welcome_email(:forward, state, opts)
  defp send_welcome_email(:rollback, state, opts)
end

MySage.execute(http_adapter: HTTPoison)
```

# Interface

```
@type pipeline :: %{}
@type step :: %{}
@type retry_opts :: [retry_limit: 3, retry_backoff: :exponential, retry_base_timeout: 1000]
@type sage_apply :: fn prev_results, state
@type sage_rollback :: fn apply_result, state

@type finally :: fn {:ok, result, other_side_effects}, state -> | fn {:error, failed_step_name :: atom}, state ->

@spec new(shared_state :: any) :: pipeline
@spec retry(pipeline :: pipeline, step :: Sage.step, opts :: retry_opts) :: pipeline
@spec run(pipeline :: pipeline, name :: atom, apply :: sage_apply, rollback :: sage_rollback) :: pipeline
@spec run_cached(pipeline :: pipeline, name :: atom, apply :: sage_apply, rollback :: sage_rollback) :: pipeline
@spec parallel(pipeline :: pipeline, parallel1 :: pipeline, parallel2 :: pipeline, ..) :: pipeline

@spec finally(result :: pipeline, callback :: function) :: pipeline # Should execute even if there is a bug in the code

@spec execute(pipeline :: pipeline) :: {:ok, result, [other_side_effects]} | {:error, reason}

@spec isolate(pipeline :: pipeline, before :: function, rollback :: function) # isolate(sage, Repo.transaction/1, Repo.rollback/1)

@spec run_multi(pipeline :: pipeline, Ecto.Multi.t, error_handler :: function) :: pipeline
@spec from_multi(Ecto.Multi.t, error_handler :: function)
@spec with_idempotency(key :: string, adapter :: module)

@spec append
@spec prepend
@spec merge(pipeline, pipeline | {m, f, a})

# Ideas
@spec abort
@spec run_async()
# run_async() |> run_async() |> run() - run() would await for all async operations to complete, instead of parallel()


@spec checkpoint(retry_opts) # Retry everything between checkpoints
@spec forward_recovery(retry_opts) # Called from a compensating transaction block, it tells how many times we want to retry upstream transactions before failure ## can be in format of returning {:retry, blah blah} from it, which makes `checkpoint/2` very simple. No tx code, but retry logic in rollback code.

@type rollback_return :: {:error, reason} # I am compensated my transactions, continue backwards-recovery
                       | {:ok, :retry} # I am compensated by transaction, let's retry
                       | {:abort, reason} # I am compensated transaction and want to force backwards recovery on all steps
```

*Error is returned from the latest compensating transaction. We don't care about compensation in between.*

```
defimpl Sagas, for: Ecto.Multi do
  @impl true
  def run(multi) do
    from_multi(multi)
  end

  defp from_multi(%Ecto.Multi{}), do: ...
end

```

# RFC's

### Checkpoints

Do not put names in `Saga.do/2`, instead use `Saga.checkpoint/3`.

### Side effects

One side effect per do or allow do's to return `{:ok, result, [side_effects]}` tuple?

### Idempotency

Sage.new()
|> Sage.with_idempotency(enabled? \\ true) // or with_persistency()
|> Sage.do(..) # Ok, result is written to a persistent storage
|> Sage.do(..) # Ok, result is written
|> Sage.do(..) # Error, probably nothing to rollback and retry of this sage would continue from this step (state is fetched from DB)
|> Sage.do(..)

### Parallel execution

```
s1 = Sage.new() |> Sage.do(...) |> Sage.do(...)
s2 = Sage.new() |> Sage.do(...) |> Sage.do(...)

Sage.new() |> Sage.do |> Sage.parallel(s1, s2) |> Sage.do(...) |> Sage.run()
```

### Before and Finally

Sometimes we want to run function on beginning and end of sage irrespectively to the individual execution stages (about the same as Ecto.Multu returns you `{:ok, result}` or error stage which is failed).

This is especially useful to init a DB transaction and rollback it after all execution stages:

```
Wrap execution in Repo.transaction and rollback it if pipeline fails
Sage.do(apply, rollback, fn _ -> Repo.transaction() end, fn {:error, reason} -> Repo.rollback() end)
```

Or you want to ack or nack to a message queue:

```
Wrap execution in Repo.transaction and rollback it if pipeline fails
Sage.do(apply, rollback, fn _ -> AMQP.ack() end, fn {:error, reason} -> AMQP.nack() end)
```

### Retries

Ability to set a retry policy for an individual stage, eg:

`Sage.do(..., retry_limit: 3, retry_backoff: :exponential, retry_base_timeout: 1000)`

### HTTP lib on top of Sage

See https://gist.github.com/michalmuskala/5cee518b918aa5a441e757efca965d22

#### Request Cache

`do_cached/2`

### Macro syntax

1.

```
Sage.new()
|-> apply()
|-< rollback()
|-> apply()
|-< rollback()
|> Sage.execute()
```

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
