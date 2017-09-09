# Sage

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

Sage allows you to use adapter to simplify integration with database and HTTP clients.

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

### HTTP calls


### Ecto & Database

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
