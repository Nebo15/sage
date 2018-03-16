# Sage

[![Deps Status](https://beta.hexfaktor.org/badge/all/github/Nebo15/sage.svg)](https://beta.hexfaktor.org/github/Nebo15/sage) [![Inline docs](http://inch-ci.org/github/nebo15/sage.svg)](http://inch-ci.org/github/nebo15/sage) [![Build Status](https://travis-ci.org/Nebo15/sage.svg?branch=master)](https://travis-ci.org/Nebo15/sage) [![Coverage Status](https://coveralls.io/repos/github/Nebo15/sage/badge.svg?branch=master)](https://coveralls.io/github/Nebo15/sage?branch=master) [![Ebert](https://ebertapp.io/github/Nebo15/sage.svg)](https://ebertapp.io/github/Nebo15/sage)

Sage is a dependency-free implementation of [Sagas](http://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) pattern in pure Elixir. It is a go to way when you dealing with distributed transactions, especially with
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

## Rationale (use cases)

Lot's of applications I've seen face a common task - interaction with third-party API's to offload some the work on SaaS products or micro-services.

Sometimes you simply need to interact with more than one database.

The case when you can't get a proper transaction isolation (that we all got used to thanks to RDBMS).
You'll need to clean up all side effects to not leave a database in an inconsistent state, when
application code has failed in the middle of a transaction.

Consider following pseudo-code (don't do this):

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
3. Does not cover retries, async operations or circuit breaker;
4. Can not keep things together, because `with` returns are not available in the `else` block;
5. No error handling in the case our code has bugs;
5. Hard to test.

Of course, you can manage that by splitting `create_and_subscribe_user/1`, but the resulting code would be significantly larger and would be much harder to maintain.

Instead, let's see how that pipeline would look with `Sage`:

```elixir
import Sage

new()
|> run(:user, &create_user/2)
|> run(:plans, &fetch_subscription_plans/2)
|> Sage.Experimental.checkpoint(retry_limit: 3) # If one of the next transactions fails, retry them 3 times max
|> run(:subscription, &create_subscription/2, &delete_subscription/3)
|> run_async(:delivery, &schedule_delivery/2, &delete_delivery_from_schedule/3)
|> run_async(:receipt, &send_email_receipt/2, &send_excuse_for_email_receipt/3)
|> run(:update_user, &set_plan_for_a_user/2)
|> finally(&acknowledge_job/2)
|> to_function(attrs)
|> Repo.transaction()
```

Along with more readable code, you are getting:

- Guarantees that all transaction steps are completed or all failed steps are compensated;
- Much simpler and easier to test a code of a transaction and compensation functions implementations;
- Retries, circuit breaking and asynchronous requests;
- Declarative way to define your transactions and run them.

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

But if that's not enough for you, it is possible to register handler via `on_compensation_error/2`.
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
    {:sage, "~> 0.3.3"}
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
