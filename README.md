# Sage

Sage is an implementation of [Sagas](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) pattern
in pure Elixir. It is go to way when you dealing with distributed transactions, especially with
an error recovery/cleanup. Sagas guarantees that either all the transactions in a saga are
successfully completed or compensating transactions are run to amend a partial execution.

> It’s like `Ecto.Multi` but across business logic and third-party APIs.
>
> -- <cite>@jayjun</cite>

This is done by defining two way flow with transaction and compensation functions, if one of transactions fails Sage will do it's beset to make sure that it's and all predecessors compensations are completed.

To visualize, let's imagine we have a 4-step transaction. Successful flow should look something like this:
```
[T1] -> [T2] -> [T3] -> [T4]
```

and if we have a failure on 3-d step Sage will cleanup side effects by running compensation functions:

```
[T1] -> [T2] -> [T3 has an error]
                ↓
[C1] <- [C2] <- [C3]
```

## Additional Features

Along with that simple idea, you will get much more out of the box with Sage:

- Transaction retries;
- TTL-based caching;
- Transaction timeouts;
- Asynchronous transactions;
- Ease to write circuit breakers;
- Code that is clean and easy to test;
- Low cost of integration in existing code base and low performance overhead;
- Ability to don't lock the database for long running transactions;
- Extensibility - write your own handler for critical errors or metric collector to measure how much time each step took.

## Rationale

Lot's of applications I've seen face a common task - interaction with other API's to offload some work to third-party SaaS products or micro-services. Also, sometimes you simply need to interact with more than one database.

In those cases you can't get transaction isolation that we all are used to thanks to RDBMS.
When failing in the middle of transaction now it's the developer responsibility to make sure
we cleaned out all side effects to don't leave application in inconsistent state.

Consider following pseudo-code:

```elixir
defmodule WithSagas do
  def create_and_subscribe_user(attrs) do
    Repo.transaction(fn ->
      with {:ok, user} <- create_user(attrs),
           {:ok, plans} <- fetch_subscription_plans(attrs),
           {:ok, charge} <- charge_card(user, subscription),
           {:ok, subscription} <- create_subscription(user, plan, attrs),
           {:ok, _delivery} <- schedule_delivery(user, subscription, attrs),
           {:ok, _receipt} <- send_email_receipt(user, subscription, attrs),
           {:ok, user} <- update_user(user, %{subscription: subscription}) do
        acknowledge_job(opts)
      else
        {:error, {:carge_failed, _reason}} ->
          # First problem: charge is not available here
          :ok = refund(charge)
          reject_job(opts)

        {:error, {:create_subscription, _reason}} ->
          # Second problem: growing list of compensations
          :ok = refund(charge)
          :ok = delete_subscription(subscription)
          reject_job(opts)

        # Third problem: how to know should be send another email, at which stage we failed?

        other ->
          # Will rollback transaction on all other errors
          :ok = ensure_deleted(fn -> refund(charge) end)
          :ok = ensure_deleted(fn -> delete_subscription(subscription) end)
          :ok = ensure_deleted(fn -> delete_delivery_from_schedule(delivery) end)
          reject_job(opts)

          other
      end
    end)
  end

  defp ensure_deleted(cb) do
    case cb.() do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  end
end
```

Along with issues highlighted in code itself, there are few more:

1. To know at which stage we failed we need to keep eye on special returns from functions we are using here;
2. Hard to control that for all cases there is a condition to compensate then;
3. Does not cover retries, async operations or circuit breaker;
4. Can not keep things together, because `with` returns are not available in `else` block;
5. No error handling in case of code bugs;
5. Hard to test.

Of course, you can manage that by splitting `create_and_subscribe_user/1`, but the resulting code would take more space and be harder to maintain.

Instead, let's see how this pipeline would look with `Sage`:

```elixir
import Sage

new()
|> run(:user, &create_user/2)
|> run_cached(:plans, &fetch_subscription_plans/3)
|> checkpoint(retry_limit: 3) # Retry everything after a checkpoint 3 times (if anything fails), `retry_timeout` is taken from `attrs`
|> run(:subscription, &create_subscription/2, &delete_subscription/3)
|> run_async(:delivery, &schedule_delivery/2, &delete_delivery_from_schedule/3)
|> run_async(:receipt, &send_email_receipt/2, &send_excuse_for_email_receipt/3)
|> run(:update_user, &set_plan_for_a_user/2)
|> finally(&acknowledge_job/2)
|> to_function(attrs)
|> Repo.transaction()
```

Along with more readable code, you getting:

- guarantees that transaction steps are completed or all failed steps are compensated;
- much simpler and easier to test code for transaction and compensation function implementations;
- retries, caching, circuit breaking and asynchronous requests;
- declarative way to define your transactions and run them.

## Critical Error Handling

### For Transactions

Transactions are wrapped in a `try..catch` block.
Whenever a critical error occurs (exception is raised, exit signal is received or function has an unexpected return)
Sage will run all compensations and then reraise exception, so you would see it like it occurred without Sage.

### For Compensations

By default, compensations are not protected from critical errors and would raise an exception.
This is done to keep simplicity and follow "let it fall" pattern of the language,
thinking that this kind of errors should be logged and then manually investigated by a developer.

But if that's not enough for you, it is possible to register handler via `on_compensation_error/2`.
When it's registered, compensations are wrapped in a `try..catch` block
and then it's error handler responsibility to take care about further actions. Few solutions you might want to try:

- Send notification to a Slack channel about need of manual resolution;
- Retry compensation;
- Spin off a new supervised process that would retry compensation and return an error in the Sage.
(Useful when you have connection issues that would be resolved at some point in future.)

Logging for compensation errors is pretty verbose to drive the attention to the problem from system maintainers.

## For `finally/2` callback

Sage does it's best to make sure final callback is executed even if there is a program bug in the code.
This guarantee simplifies integration with a job processing queues, you can read more about it at [GenTask Readme](https://github.com/Nebo15/gen_task).

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
