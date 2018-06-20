# Sage

[![Deps Status](https://beta.hexfaktor.org/badge/all/github/Nebo15/sage.svg)](https://beta.hexfaktor.org/github/Nebo15/sage) [![Inline docs](http://inch-ci.org/github/nebo15/sage.svg)](http://inch-ci.org/github/nebo15/sage) [![Build Status](https://travis-ci.org/Nebo15/sage.svg?branch=master)](https://travis-ci.org/Nebo15/sage) [![Coverage Status](https://coveralls.io/repos/github/Nebo15/sage/badge.svg?branch=master)](https://coveralls.io/github/Nebo15/sage?branch=master) [![Ebert](https://ebertapp.io/github/Nebo15/sage.svg)](https://ebertapp.io/github/Nebo15/sage)

Sage is a dependency-free implementation of [Sagas](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) pattern in pure Elixir and provides set of features on top of.

It is a go to way when you dealing with distributed transactions, especially with
an error recovery/cleanup. Sage does it's best to guarantee that either all of the transactions in a saga are
successfully completed or compensating that all of the transactions did run to amend a partial execution.

> It’s like `Ecto.Multi` but across business logic and third-party APIs.
>
> -- <cite>@jayjun</cite>

This is done by defining two way flow with transaction and compensation functions. When one of the transactions fails, Sage will ensure that transaction's and all of it's predecessors compensations are executed. However, it's important to note that Sage can not protect you from a node failure that executes given Sage.

To visualize it, let's imagine we have a 4-step transaction. Successful execution flow would look like:
```
[T1] -> [T2] -> [T3] -> [T4]
```

and if we get a failure on 3-d step, Sage would cleanup side effects by running compensation functions:

```
[T1] -> [T2] -> [T3 has an error]
                ↓
[C1] <- [C2] <- [C3]
```

## Additional Features

Along with that simple idea, you will get much more out of the box with Sage:

- Transaction retries;
- Asynchronous transactions with timeout;
- Retries with exponential backoff and jitter;
- Ease to write circuit breakers;
- Code that is clean and easy to test;
- Low cost of integration in existing code base and low performance overhead;
- Ability to not lock the database with long running transactions;
- Extensibility - write your own handler for critical errors or metric collector to measure how much time each step took.

## Goals

- Become a defacto tool to run distributed transactions in Elixir world;
- Stay simple to use and small to maintain, less code - less bugs;
- Educate people how to run distributed transaction pragmatically.

## Rationale (use cases)

Lot's of applications I've seen face a common task - interaction with third-party API's to offload some the work on
SaaS products or micro-services, when you simply need to commit to more than one database or in all other cases where
you don't have transaction isolation between business logic steps (that we all got used to thanks to RDBMS).

When dealing with those, it is a common desire to handle all sorts of errors when application code has failed
in the middle of a transaction so that you won't leave databases in an inconsistent state.

### Using `with` (the old way)

One of solutions is to write a business logic using `with` syntax. But when number of transaction steps grow,
code becomes hard to maintain, test and even looks ugly. Consider following pseudo-code (don't do this):

```elixir
defmodule WithExample do
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
        {:error, {:charge_failed, _reason}} ->
          # First problem: charge is not available here
          :ok = refund(charge)
          reject_job(opts)

        {:error, {:create_subscription, _reason}} ->
          # Second problem: growing list of compensations
          :ok = refund(charge)
          :ok = delete_subscription(subscription)
          reject_job(opts)

        # Third problem: how to decide when we should be sending another email or
        # at which stage we've failed?

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

Along with the issues highlighted in the code itself, there are few more:

1. To know at which stage we failed we need to keep an eye on the special returns from the functions we're using here;
2. Hard to control that there is a condition to compensate for all possible error cases;
3. Impossible not keep relative code close to each other, because bare expressions in `with`do not leak to the `else` block;
4. Hard to test;
5. Hard to improve, eg. it is hard to add retries, async operations or circuit breaker without making it even worse.

For some time you might get away by splitting `create_and_subscribe_user/1`, but it only works while number of transactions is very small.

### Using Sagas

Instead, let's see how that pipeline would look with `Sage`:

```elixir
defmodule SageExample do
  import Sage
  require Logger

  @spec create_and_subscribe_user(attrs :: map()) :: {:ok, last_effect :: any(), all_effects :: map()} | {:error, reason :: any()}
  def create_and_subscribe_user(attrs) do
    new()
    |> run(:user, &create_user/2)
    |> run(:plans, &fetch_subscription_plans/2, &subscription_plans_circuit_breaker/4)
    |> run(:subscription, &create_subscription/2, &delete_subscription/4)
    |> run_async(:delivery, &schedule_delivery/2, &delete_delivery_from_schedule/4)
    |> run_async(:receipt, &send_email_receipt/2, &send_excuse_for_email_receipt/4)
    |> run(:update_user, &set_plan_for_a_user/2)
    |> finally(&acknowledge_job/2)
    |> transaction(SageExample.Repo, attrs)
  end

  # Transaction behaviour:
  # @callback transaction(attrs :: map()) :: {:ok, last_effect :: any(), all_effects :: map()} | {:error, reason :: any()}

  # Compensation behaviour:
  # @callback compensation(
  #             effect_to_compensate :: any(),
  #             effects_so_far :: map(),
  #             {failed_stage_name :: atom(), failed_value :: any()},
  #             attrs :: any()
  #           ) :: :ok | :abort | {:retry, retry_opts :: Sage.retry_opts()} | {:continue, any()}

  def create_user(_effects_so_far, %{"user" => user_attrs}) do
    %SageExample.User{}
    |> SageExample.User.changeset(user_attrs)
    |> SageExample.Repo.insert()
  end

  def fetch_subscription_plans(_effects_so_far, _attrs) do
    {:ok, _plans} = SageExample.Billing.APIClient.list_plans()
  end

  # If we failed to fetch plans, let's continue with cached ones
  def subscription_plans_circuit_breaker(_effect_to_compensate, _effects_so_far, _failed_stage, _attrs) do
    {:continue, [%{"id" => "free", "total" => 0}, %{"id" => "standard", "total" => 4.99}]}
  end

  def create_subscription(%{user: user}, %{"subscription" => subscription}) do
    {:ok, subscription} = SageExample.Billing.APIClient.subscribe_user(user, subscription["plan"])
  end

  def delete_subscription(_effect_to_compensate, %{user: user}, _failed_stage, _attrs) do
    :ok = SageExample.Billing.APIClient.delete_all_subscriptions_for_user(user)
    # We want to apply forward compensation from :subscription stage for 5 times
    {:retry, retry_limit: 5, base_backoff: 10, max_backoff: 30_000, enable_jitter: true}
  end

  # .. other transaction and compensation callbacks

  def acknowledge_job(:ok, attrs) do
    Logger.info("Successfully created user #{attrs["user"]["email"]}")
  end

  def acknowledge_job(_error, attrs) do
    Logger.warn("Failed to create user #{attrs["user"]["email"]}")
  end
end
```

Along with a readable code, you are getting:

- Reasonable guarantees that all transaction steps are completed or all failed steps are compensated;
- Code which is much simpler and easier to test a code;
- Retries, circuit breaking and asynchronous requests our of the box;
- Declarative way to define your transactions and run them.

Testing is easier, because instead of one monstrous function you will have many small callbacks which are easy to cover
with unit tests. You only need to tests business logic in transactions and that compensations are able to cleanup their
effects. The Sage itself has 100% test coverage.

Even more, it is possible to apply a new kind of architecture in an Elixir project where Phoenix contexts
(or just application domains) are providing helper functions for building sagas to a controller, which then
uses one or more of them to make sure that each request is side-effects free. Simplified example:

```elixir
defmodule SageExample.UserController do
  use SageExample.Web, :controller

  action_fallback SageExample.FallbackController

  def signup_and_accept_team_invitation(conn, attrs) do
    Sage.new()
    |> SageExample.Users.Sagas.create_user()
    |> SageExample.Teams.Sagas.accept_invitation()
    |> SageExample.Billing.Sagas.prorate_team_size()
    |> Sage.execute(attrs)
  end
end
```

If you want to have more examples check out [blog post on Sage](https://medium.com/nebo-15/introducing-sage-a-sagas-pattern-implementation-in-elixir-3ad499f236f6).

## Execution Guarantees and Edge Cases

While Sage will do its best to compensate failures in a transaction and leave a system in a consistent state, there are some edge cases where it wouldn't be possible.

1. What if my transaction has bugs or other errors?

    Transactions are wrapped in a `try..catch` block and would tolerate any exception, exit or rescue. And after executing compensations, an error will be reraised.

2. What if my compensation has bugs or other errors?

    By default, compensations would not try to handle any kinds of errors. But you can write an adapter to handle those. For more information see [Critical Error Handling](https://github.com/Nebo15/sage#for-compensations) section.

3. What if the process that executes Sage or whole node fails?

    Right now Sage doesn't provide a way to tolerate failures of executing processes. (However, there is an [RFC that aims for that](https://github.com/Nebo15/sage/issues/9).)

4. What if an external API call fails and it's impossible to revert a step?

    In such cases, the process which is handling the pipeline will crash and the exception will be thrown. Make sure that you have a way of reacting to such cases (in some cases it might be acceptable to ignore the error while others might require a manual intervention).

5. Can I be absolutely sure that everything went well?

    Unfortunately, no. As with any other distributed system, messages could be lost, network could go down, hardware could fail etc. There are no way to programmatically solve all those cases, even retrying compensations won't help in some of such cases.

For example, it's possible that a reply from an external API is lost even though a request actually succeeded. In such cases, you might want to retry the compensation which might have an unexpected result. Best way to solve that issue is to [write compensations in an idempotent way](https://hexdocs.pm/sage/Sage.html#t:compensation/0) and to always make sure that you have proper monitoring tools in place.

## Critical Error Handling

### For Transactions

Transactions are wrapped in a `try..catch` block.

Whenever a critical error occurs (exception is raised, error thrown or exit signal is received)
Sage will run all compensations and then reraise the exception with the same stacktrace,
so your log would look like it occurred without using a Sage.

### For Compensations

By default, compensations are not protected from critical errors and would raise an exception.
This is done to keep simplicity and follow "let it fall" pattern of the language,
thinking that this kind of errors should be logged and then manually investigated by a developer.

But if that's not enough for you, it is possible to register handler via `with_compensation_error_handler/2`.
When it's registered, compensations are wrapped in a `try..catch` block
and then it's error handler responsibility to take care about further actions. Few solutions you might want to try:

- Send notification to a Slack channel about need of manual resolution;
- Retry compensation;
- Spawn a new supervised process that would retry compensation and return an error in the Sage.
(Useful when you have connection issues that would be resolved at some point in future.)

Logging for compensation errors is pretty verbose to drive the attention to the problem from system maintainers.

## `finally/2` hook

Sage does it's best to make sure final callback is executed even if there is a program bug in the code.
This guarantee simplifies integration with a job processing queues, you can read more about it at [GenTask Readme](https://github.com/Nebo15/gen_task).

If an error is raised within `finally/2` hook, it's getting logged and ignored. Follow the simple rule - everything that
is on your critical path should be a Sage transaction.

## Tracing and measuring Sage execution steps

Sage allows you to set a tracer module which is called on each step of the execution flow (before and after transactions and/or compensations). It could be used to report metrics on the execution flow.

If error is raised within tracing function, it's getting logged and ignored.

# Visualizations

For making it easier to understand what flow you should expect here are few additional examples:

1. Retries

```
[T1] -> [T2] -> [T3 has an error]
                ↓
[C2 retries] <- [C3]
        ↓
        [T2] -> [T3]
```

2. Circuit breaker
```
[T1] -> [T2  has an error]
                ↓
        [C2 circuit breaker] -> [T3]
```

2. Async transactions
```
[T1] -> [T2 async] -↓
        [T3 async] -> [await for T2 and T3 before non-async operation] -> [T4]
```

2. Error in async transaction (notice: both async operations are awaited and then compensated)
```
[T1] -> [T2 async with error] -↓
        [T3 async] -> [await for T2 and T3 before non-async operation]
                       ↓
[C1]   <- [C2]   <- [C3]
```

## Installation

The package can be installed by adding [`sage`](https://hex.pm/packages/sage) to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:sage, "~> 0.4.0"}
  ]
end
```

Documentation can be found at [https://hexdocs.pm/sage](https://hexdocs.pm/sage).

# License

See [LICENSE.md](LICENSE.md).

# Credits

Some implementation ideas were taken from [`Ecto.Multi`](https://github.com/elixir-ecto/ecto/blob/master/lib/ecto/multi.ex) module originally implemented by @michalmuskala and [`gisla`](https://github.com/mrallen1/gisla) by @mrallen1 which implements Sagas pattern for Erlang.

Sagas idea have origins from [whitepaper](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) written in 80's. There are more recent work - [Compensating Transactions](https://docs.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction) which is part of Azure Architecture Guidelines.

# Thanks to

 - Josh Forisha for letting me to use this awesome project name on hex.pm (he had a package with that name);
 - @michalmuskala, @alco and @alecnmk for giving feedback and ideas along my way;
 - all the Elixir community and Core Team. Guys, you are awesome ❤️.
