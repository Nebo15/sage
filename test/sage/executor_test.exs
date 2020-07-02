defmodule Sage.ExecutorTest do
  use Sage.EffectsCase, async: true

  describe "transactions" do
    test "are executed" do
      {hook, hook_assertion} = final_hook_with_assertion(:ok, a: :b)

      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction(:t2), compensation())
        |> run(:step3, {__MODULE__, :mfa_transaction, [transaction(:t3)]}, compensation())
        |> run({:step, 4}, transaction(:t4), compensation())
        |> finally(hook)
        |> execute(a: :b)

      hook_assertion.()
      assert_effects([:t1, :t2, :t3, :t4])
      assert result == {:ok, :t4, %{:step1 => :t1, :step2 => :t2, :step3 => :t3, {:step, 4} => :t4}}
    end

    test "are executed with Executor" do
      {hook, hook_assertion} = final_hook_with_assertion(:ok)

      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction(:t2), compensation())
        |> run(:step3, {__MODULE__, :mfa_transaction, [transaction(:t3)]}, compensation())
        |> finally(hook)
        |> Sage.Executor.execute()

      hook_assertion.()
      assert_effects([:t1, :t2, :t3])
      assert result == {:ok, :t3, %{step1: :t1, step2: :t2, step3: :t3}}
    end

    test "are awaiting on next synchronous operation when executes asynchronous transactions" do
      {hook, hook_assertion} = final_hook_with_assertion(:ok, a: :b)

      result =
        new()
        |> run_async(:step1, transaction(:t1), not_strict_compensation())
        |> run_async(:step2, transaction(:t2), not_strict_compensation())
        |> run(:step3, fn effects_so_far, opts ->
          assert effects_so_far == %{step1: :t1, step2: :t2}
          transaction(:t3).(effects_so_far, opts)
        end)
        |> finally(hook)
        |> execute(a: :b)

      hook_assertion.()
      assert_effect(:t1)
      assert_effect(:t2)
      assert_effect(:t3)

      assert result == {:ok, :t3, %{step1: :t1, step2: :t2, step3: :t3}}
    end

    test "are returning last operation result when executes asynchronous transactions on last steps" do
      {hook, hook_assertion} = final_hook_with_assertion(:ok, a: :b)

      result =
        new()
        |> run_async(:step1, transaction(:t1), not_strict_compensation())
        |> run_async(:step2, transaction(:t2), not_strict_compensation())
        |> run_async(:step3, transaction(:t3), not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run_async(:step5, transaction(:t5), not_strict_compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_effect(:t1)
      assert_effect(:t2)
      assert_effect(:t3)
      assert_effect(:t4)
      assert_effect(:t5)

      hook_assertion.()

      assert result == {:ok, :t5, %{step1: :t1, step2: :t2, step3: :t3, step4: :t4, step5: :t5}}
    end
  end

  test "effects are not compensated for operations with :noop compensation" do
    {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)

    result =
      new()
      |> run(:step1, transaction_with_error(:t1))
      |> finally(hook)
      |> execute(a: :b)

    assert_effects([:t1])
    assert result == {:error, :t1}
    hook_assertion.()
  end

  test "effects are not compensated for async operations with :noop compensation" do
    {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)

    result =
      new()
      |> run_async(:step1, transaction_with_error(:t1), :noop)
      |> finally(hook)
      |> execute(a: :b)

    assert_effects([:t1])
    assert result == {:error, :t1}
    hook_assertion.()
  end

  test "asynchronous transactions process metadata is copied from parent process" do
    test_pid = self()
    metadata = [test_pid: test_pid, test_ref: make_ref()]
    Logger.metadata(metadata)

    tx = fn effects_so_far, opts ->
      send(test_pid, {:logger_metadata, Logger.metadata()})
      transaction(:t1).(effects_so_far, opts)
    end

    result =
      new()
      |> run_async(:step1, tx, not_strict_compensation())
      |> execute()

    assert result == {:ok, :t1, %{step1: :t1}}
    assert_receive {:logger_metadata, ^metadata}
  end

  describe "effects are compensated" do
    test "when transaction fails" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)

      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction(:t2), compensation())
        |> run(:step3, transaction_with_error(:t3), compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_no_effects()
      assert result == {:error, :t3}
      hook_assertion.()
    end

    test "when compensation is mfa tuple" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)

      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction(:t2), compensation())
        |> run(:step3, transaction_with_error(:t3), {__MODULE__, :mfa_compensation, [compensation(:t3)]})
        |> finally(hook)
        |> execute(a: :b)

      assert_no_effects()
      assert result == {:error, :t3}
      hook_assertion.()
    end

    test "for executed async transactions when transaction fails" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)
      test_pid = self()

      cmp = fn effect_to_compensate, effects_so_far, opts ->
        send(test_pid, {:compensate, effect_to_compensate})
        not_strict_compensation().(effect_to_compensate, effects_so_far, opts)
      end

      result =
        new()
        |> run_async(:step1, transaction(:t1), cmp)
        |> run_async(:step2, transaction(:t2), cmp)
        |> run(:step3, transaction_with_error(:t3), cmp)
        |> finally(hook)
        |> execute(a: :b)

      assert_receive {:compensate, :t2}
      assert_receive {:compensate, :t1}

      assert_no_effects()
      assert result == {:error, :t3}
      hook_assertion.()
    end

    test "for all started async transaction when one of them failed" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)
      test_pid = self()

      cmp = fn effect_to_compensate, effects_so_far, opts ->
        send(test_pid, {:compensate, effect_to_compensate})
        not_strict_compensation().(effect_to_compensate, effects_so_far, opts)
      end

      result =
        new()
        |> run_async(:step1, transaction_with_error(:t1), cmp)
        |> run_async(:step2, transaction(:t2), cmp)
        |> run_async(:step3, transaction(:t3), cmp)
        |> finally(hook)
        |> execute(a: :b)

      # Since all async tasks are run in parallel
      # after one of them failed, we await for rest of them
      # and compensate their effects
      assert_receive {:compensate, :t2}
      assert_receive {:compensate, :t1}
      assert_receive {:compensate, :t3}

      assert_no_effects()
      assert result == {:error, :t1}

      hook_assertion.()
    end

    test "with preserved error name and reason for synchronous transactions" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)

      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction_with_error(:t2), compensation())
        |> run(:step3, transaction_with_error(:t3), compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_no_effects()
      assert result == {:error, :t2}

      hook_assertion.()
    end

    test "with preserved error name and reason for asynchronous transactions" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)

      cmp = fn effect_to_compensate, effects_so_far, opts ->
        not_strict_compensation().(effect_to_compensate, effects_so_far, opts)
      end

      result =
        new()
        |> run_async(:step1, transaction(:t1), cmp)
        |> run_async(:step2, transaction_with_error(:t2), cmp)
        |> run_async(:step3, transaction(:t3), cmp)
        |> finally(hook)
        |> execute(a: :b)

      assert_no_effects()
      assert result == {:error, :t2}

      hook_assertion.()
    end

    test "when transaction raised an exception" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)

      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      assert_raise RuntimeError, "error while creating t5", fn ->
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, transaction(:t2), compensation())
        |> run_async(:step3, tx, not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run(:step5, transaction_with_exception(:t5), compensation(:t5))
        |> finally(hook)
        |> execute()
      end

      # Retries are applying when exception is raised
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()

      hook_assertion.()
    end

    test "when transaction raised a clause error" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)

      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      transaction_with_clause_error = fn %{unexpected: :pattern}, _opts ->
        {:ok, :next}
      end

      assert_raise FunctionClauseError, ~r[no function clause matching in anonymous fn/2], fn ->
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, transaction(:t2), compensation())
        |> run_async(:step3, tx, not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run(:step5, transaction_with_clause_error)
        |> finally(hook)
        |> execute()
      end

      # Retries are applying when exception is raised
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()

      hook_assertion.()
    end

    test "when transaction throws an error" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)

      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      assert catch_throw(
               new()
               |> run(:step1, transaction(:t1), compensation_with_retry(3))
               |> run(:step2, transaction(:t2), compensation())
               |> run_async(:step3, tx, not_strict_compensation())
               |> run_async(:step4, transaction(:t4), not_strict_compensation())
               |> run(:step5, transaction_with_throw(:t5), compensation(:t5))
               |> finally(hook)
               |> execute()
             ) == "error while creating t5"

      # Retries are applying when error is thrown
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end

    test "when transaction exits" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      assert catch_exit(
               new()
               |> run(:step1, transaction(:t1), compensation_with_retry(3))
               |> run(:step2, transaction(:t2), compensation())
               |> run_async(:step3, tx, not_strict_compensation())
               |> run_async(:step4, transaction(:t4), not_strict_compensation())
               |> run(:step5, transaction_with_exit(:t5), compensation(:t5))
               |> finally(hook)
               |> execute()
             ) == "error while creating t5"

      # Retries are applying when tx exited
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end

    test "when transaction has malformed return" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      message = ~r"""
      ^expected transaction .* to return
      {:ok, effect}, {:error, reason} or {:abort, reason}, got:

        {:bad_returns, :are_bad_mmmkay}$
      """

      assert_raise Sage.MalformedTransactionReturnError, message, fn ->
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, transaction(:t2), compensation())
        |> run_async(:step3, tx, not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run(:step5, transaction_with_malformed_return(:t5), compensation(:t5))
        |> finally(hook)
        |> execute()
      end

      # Retries are applying when tx exited
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end

    test "when async transaction timed out" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      message = """
      asynchronous transaction for operation step5 has timed out,
      expected it to return within 10 microseconds
      """

      assert_raise Sage.AsyncTransactionTimeoutError, message, fn ->
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(2))
        |> run(:step2, transaction(:t2), compensation())
        |> run_async(:step3, tx, not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run_async(:step5, transaction_with_sleep(:t5, 20), not_strict_compensation(:t5), timeout: 10)
        |> finally(hook)
        |> execute()
      end

      # Retries are applying when tx timed out
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end

    test "when async transaction raised an exception" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      assert_raise RuntimeError, "error while creating t5", fn ->
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, transaction(:t2), compensation())
        |> run_async(:step3, tx, not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run_async(:step5, transaction_with_exception(:t5), not_strict_compensation(:t5))
        |> finally(hook)
        |> execute()
      end

      # Retries are applying when exception is raised
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end

    test "when async transaction throws an error" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      assert catch_throw(
               new()
               |> run(:step1, transaction(:t1), compensation_with_retry(3))
               |> run(:step2, transaction(:t2), compensation())
               |> run_async(:step3, tx, not_strict_compensation())
               |> run_async(:step4, transaction(:t4), not_strict_compensation())
               |> run_async(:step5, transaction_with_throw(:t5), not_strict_compensation(:t5))
               |> finally(hook)
               |> execute()
             ) == "error while creating t5"

      # Retries are applying when error is thrown
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end

    test "when async transaction exits" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      assert catch_exit(
               new()
               |> run(:step1, transaction(:t1), compensation_with_retry(3))
               |> run(:step2, transaction(:t2), compensation())
               |> run_async(:step3, tx, not_strict_compensation())
               |> run_async(:step4, transaction(:t4), not_strict_compensation())
               |> run_async(:step5, transaction_with_exit(:t5), not_strict_compensation(:t5))
               |> finally(hook)
               |> execute()
             ) == "error while creating t5"

      # Retries are applying when tx exited
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end

    test "when async transaction has malformed return" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      message = ~r"""
      ^expected transaction .* to return
      {:ok, effect}, {:error, reason} or {:abort, reason}, got:

        {:bad_returns, :are_bad_mmmkay}$
      """

      assert_raise Sage.MalformedTransactionReturnError, message, fn ->
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, transaction(:t2), compensation())
        |> run_async(:step3, tx, not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run_async(:step5, transaction_with_malformed_return(:t5), not_strict_compensation(:t5))
        |> finally(hook)
        |> execute()
      end

      # Retries are applying when tx exited
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      assert_no_effects()
      hook_assertion.()
    end
  end

  test "errors in compensations are raised by default" do
    {hook, _hook_assertion} = final_hook_with_assertion(:error)
    test_pid = self()
    tx = transaction(:t3)

    tx = fn effects_so_far, opts ->
      send(test_pid, {:execute, :t3})
      tx.(effects_so_far, opts)
    end

    assert_raise RuntimeError, "error while compensating t5", fn ->
      new()
      |> run(:step1, transaction(:t1), compensation_with_retry(3))
      |> run(:step2, transaction(:t2), compensation())
      |> run_async(:step3, tx, not_strict_compensation())
      |> run_async(:step4, transaction(:t4), not_strict_compensation())
      |> run(:step5, transaction_with_error(:t5), compensation_with_exception(:t5))
      |> finally(hook)
      |> execute()
    end

    # Transactions are executed once
    assert_receive {:execute, :t3}
    refute_receive {:execute, :t3}

    assert_effect(:t1)
    assert_effect(:t2)
    assert_effect(:t3)
    assert_effect(:t4)

    refute_received {:finally, _, _, _}
  end

  test "clause exceptions in compensations are raised and preserved" do
    {hook, _hook_assertion} = final_hook_with_assertion(:error)
    test_pid = self()
    tx = transaction(:t3)

    tx = fn effects_so_far, opts ->
      send(test_pid, {:execute, :t3})
      tx.(effects_so_far, opts)
    end

    error_prone_compensation = fn _effect_to_compensate, _effects_so_far, _opts ->
      :unknown_module.call(:arg)

      :ok
    end

    assert_raise UndefinedFunctionError,
                 "function :unknown_module.call/1 is undefined (module :unknown_module is not available)",
                 fn ->
                   new()
                   |> run(:step1, transaction(:t1), compensation_with_retry(3))
                   |> run(:step2, transaction(:t2), compensation())
                   |> run_async(:step3, tx, not_strict_compensation())
                   |> run_async(:step4, transaction(:t4), not_strict_compensation())
                   |> run(:step5, transaction_with_error(:t5), error_prone_compensation)
                   |> finally(hook)
                   |> execute()
                 end

    # Transactions are executed once
    assert_receive {:execute, :t3}
    refute_receive {:execute, :t3}

    assert_effect(:t1)
    assert_effect(:t2)
    assert_effect(:t3)
    assert_effect(:t4)

    refute_received {:finally, _, _, _}
  end

  test "compensations malformed return is reported by default" do
    {hook, _hook_assertion} = final_hook_with_assertion(:error, a: :b)
    test_pid = self()
    tx = transaction(:t3)

    tx = fn effects_so_far, opts ->
      send(test_pid, {:execute, :t3})
      tx.(effects_so_far, opts)
    end

    message = ~r"""
    ^expected compensation .* to return
    :ok, :abort, {:retry, retry_opts} or {:continue, effect}, got:

      {:bad_returns, :are_bad_mmmkay}$
    """

    assert_raise Sage.MalformedCompensationReturnError, message, fn ->
      new()
      |> run(:step1, transaction(:t1), compensation_with_retry(3))
      |> run(:step2, transaction(:t2), compensation())
      |> run_async(:step3, tx, not_strict_compensation())
      |> run_async(:step4, transaction(:t4), not_strict_compensation())
      |> run(:step5, transaction_with_error(:t5), compensation_with_malformed_return(:t5))
      |> finally(hook)
      |> execute()
    end

    # Transactions are executed once
    assert_receive {:execute, :t3}
    refute_receive {:execute, :t3}

    assert_effect(:t1)
    assert_effect(:t2)
    assert_effect(:t3)
    assert_effect(:t4)

    refute_received {:finally, _, _, _}
  end

  describe "compensation error handler" do
    test "can resume compensation on exception" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      new()
      |> run(:step1, transaction(:t1), compensation_with_retry(3))
      |> run(:step2, transaction(:t2), :noop)
      |> run_async(:step3, tx, not_strict_compensation())
      |> run_async(:step4, transaction(:t4), not_strict_compensation())
      |> run(:step5, transaction_with_error(:t5), compensation_with_exception(:t5))
      |> finally(hook)
      |> with_compensation_error_handler(Sage.TestCompensationErrorHandler)
      |> execute()

      # Transactions are executed once
      assert_receive {:execute, :t3}
      refute_receive {:execute, :t3}

      assert_effects([:t2])

      hook_assertion.()
    end

    test "can resume compensation on exit" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      new()
      |> run(:step1, transaction(:t1), compensation_with_retry(3))
      |> run(:step2, transaction(:t2), compensation())
      |> run_async(:step3, tx, not_strict_compensation())
      |> run_async(:step4, transaction(:t4), not_strict_compensation())
      |> run(:step5, transaction_with_error(:t5), compensation_with_exit(:t5))
      |> finally(hook)
      |> with_compensation_error_handler(Sage.TestCompensationErrorHandler)
      |> execute()

      # Transactions are executed once
      assert_receive {:execute, :t3}
      refute_receive {:execute, :t3}

      assert_no_effects()

      hook_assertion.()
    end

    test "can resume compensation on throw" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      new()
      |> run(:step1, transaction(:t1), compensation_with_retry(3))
      |> run(:step2, transaction(:t2), compensation())
      |> run_async(:step3, tx, not_strict_compensation())
      |> run_async(:step4, transaction(:t4), not_strict_compensation())
      |> run(:step5, transaction_with_error(:t5), compensation_with_throw(:t5))
      |> finally(hook)
      |> with_compensation_error_handler(Sage.TestCompensationErrorHandler)
      |> execute()

      # Transactions are executed once
      assert_receive {:execute, :t3}
      refute_receive {:execute, :t3}

      assert_no_effects()

      hook_assertion.()
    end

    test "can resume compensation on malformed return" do
      {hook, hook_assertion} = final_hook_with_assertion(:error)
      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      new()
      |> run(:step1, transaction(:t1), compensation_with_retry(3))
      |> run(:step2, transaction(:t2), compensation())
      |> run_async(:step3, tx, not_strict_compensation())
      |> run_async(:step4, transaction(:t4), not_strict_compensation())
      |> run(:step5, transaction_with_error(:t5), compensation_with_malformed_return(:t5))
      |> finally(hook)
      |> with_compensation_error_handler(Sage.TestCompensationErrorHandler)
      |> execute()

      # Transactions are executed once
      assert_receive {:execute, :t3}
      refute_receive {:execute, :t3}

      assert_no_effects()

      hook_assertion.()
    end

    test "does not execute predecessors compensations when exeception is raised and no error handler" do
      sage =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction_with_error(:t2), compensation_with_exception())
        |> run(:step3, transaction(:t3), compensation())

      assert_raise RuntimeError, "error while compensating ", fn ->
        execute(sage, %{})
      end

      assert_effect(:t1)
      assert_effect(:t2)
      refute_effect(:t3)
    end
  end

  test "compensation receives effects so far" do
    cmp1 = fn effect_to_compensate, effects_so_far, opts ->
      assert effects_so_far == %{}
      not_strict_compensation().(effect_to_compensate, effects_so_far, opts)
    end

    cmp2 = fn effect_to_compensate, effects_so_far, opts ->
      assert effects_so_far == %{step1: :t1}
      not_strict_compensation().(effect_to_compensate, effects_so_far, opts)
    end

    cmp3 = fn effect_to_compensate, effects_so_far, opts ->
      assert effects_so_far == %{step1: :t1, step2: :t2}
      compensation().(effect_to_compensate, effects_so_far, opts)
    end

    result =
      new()
      |> run_async(:step1, transaction(:t1), cmp1)
      |> run_async(:step2, transaction(:t2), cmp2)
      |> run(:step3, transaction_with_error(:t3), cmp3)
      |> execute(a: :b)

    assert_no_effects()
    assert result == {:error, :t3}
  end

  describe "tracers" do
    test "are notified for all operations" do
      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction(:t2), compensation())
        |> run_async(:step3, transaction(:t3), not_strict_compensation())
        |> run_async(:step4, transaction(:t4), not_strict_compensation())
        |> run(:step5, transaction_with_error(:t5), compensation())
        |> with_tracer(Sage.TestTracer)
        |> execute(test_pid: self())

      assert_no_effects()
      assert result == {:error, :t5}

      for step <- [:step1, :step2, :step3, :step4, :step5] do
        assert_receive {^step, :start_transaction, _tracing_state}
        assert_receive {^step, :finish_transaction, time_taken, _tracing_state}
        assert div(System.convert_time_unit(time_taken, :native, :microsecond), 100) / 10 > 0.9
      end

      for step <- [:step1, :step2, :step3, :step4, :step5] do
        assert_receive {^step, :start_compensation, _tracing_state}
        assert_receive {^step, :finish_compensation, time_taken, _tracing_state}
        assert div(System.convert_time_unit(time_taken, :native, :microsecond), 100) / 10 > 0.9
      end
    end
  end

  describe "final hooks" do
    test "can be omitted" do
      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> execute()

      assert_effects([:t1])
      assert result == {:ok, :t1, %{step1: :t1}}
    end

    test "are triggered on successes" do
      test_pid = self()

      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> finally(&{send(test_pid, &1), &2})
        |> finally({__MODULE__, :do_send, [test_pid]})
        |> execute(a: :b)

      assert_receive :ok
      assert_receive :ok

      assert_effects([:t1])
      assert result == {:ok, :t1, %{step1: :t1}}
    end

    test "are triggered on failure" do
      test_pid = self()

      result =
        new()
        |> run(:step1, transaction_with_error(:t1), compensation())
        |> finally(&{send(test_pid, &1), &2})
        |> finally({__MODULE__, :do_send, [test_pid]})
        |> execute(a: :b)

      assert_receive :error
      assert_receive :error

      assert_no_effects()
      assert result == {:error, :t1}
    end

    test "exceptions, throws and exits are logged" do
      import ExUnit.CaptureLog

      test_pid = self()

      final_hook_with_raise = fn status, _opts ->
        send(test_pid, status)
        raise "Final hook raised an exception"
      end

      final_hook_with_throw = fn status, _opts ->
        send(test_pid, status)
        throw("Final hook threw an error")
      end

      final_hook_with_exit = fn status, _opts ->
        send(test_pid, status)
        exit("Final hook exited")
      end

      log =
        capture_log(fn ->
          result =
            new()
            |> run(:step1, transaction_with_error(:t1), compensation())
            |> finally(final_hook_with_raise)
            |> finally(final_hook_with_throw)
            |> finally(final_hook_with_exit)
            |> finally({__MODULE__, :final_hook_with_raise, [test_pid]})
            |> finally({__MODULE__, :final_hook_with_throw, [test_pid]})
            |> finally({__MODULE__, :final_hook_with_exit, [test_pid]})
            |> execute(a: :b)

          assert_no_effects()
          assert result == {:error, :t1}
        end)

      assert_receive :error
      assert_receive :error
      assert_receive :error
      assert_receive :error
      assert_receive :error
      assert_receive :error
      refute_receive :error

      assert log =~ "Final mfa hook raised an exception"
      assert log =~ "Final mfa hook threw an error"
      assert log =~ "Final mfa hook exited"

      assert log =~ "Final hook raised an exception"
      assert log =~ "Final hook threw an error"
      assert log =~ "Final hook exited"
    end
  end

  describe "global options" do
    test "are sent to transaction" do
      attrs = %{foo: "bar"}

      tx = fn effects_so_far, opts ->
        assert opts == attrs
        transaction(:t1).(effects_so_far, opts)
      end

      result =
        new()
        |> run(:step1, tx, compensation())
        |> execute(attrs)

      assert result == {:ok, :t1, %{step1: :t1}}

      assert_effects([:t1])
    end

    test "are sent to compensation" do
      attrs = %{foo: "bar"}

      cmp = fn effect_to_compensate, effects_so_far, opts ->
        assert opts == attrs
        compensation(:t1).(effect_to_compensate, effects_so_far, opts)
      end

      result =
        new()
        |> run(:step1, transaction_with_error(:t1), cmp)
        |> execute(attrs)

      assert_no_effects()
      assert result == {:error, :t1}
    end

    test "are sent to final hook" do
      attrs = %{foo: "bar"}

      final_hook = fn state, opts ->
        assert state == :ok
        assert opts == attrs
      end

      result =
        new()
        |> run(:step1, transaction(:t1))
        |> finally(final_hook)
        |> execute(attrs)

      assert_effects([:t1])
      assert result == {:ok, :t1, %{step1: :t1}}
    end
  end

  describe "retries" do
    test "are executing transactions again" do
      {hook, hook_assertion} = final_hook_with_assertion(:ok, a: :b)

      test_pid = self()
      tx = transaction(:t2)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t2})
        tx.(effects_so_far, opts)
      end

      result =
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, tx, compensation())
        |> run(:step3, transaction_with_n_errors(2, :t3), compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_receive {:execute, :t2}
      assert_receive {:execute, :t2}
      assert_receive {:execute, :t2}

      hook_assertion.()

      assert_effects([:t1, :t2, :t3])
      assert result == {:ok, :t3, %{step1: :t1, step2: :t2, step3: :t3}}
    end

    test "uses previous stage effects" do
      {hook, hook_assertion} = final_hook_with_assertion(:ok, a: :b)

      test_pid = self()
      tx = transaction(:t3)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t3})
        tx.(effects_so_far, opts)
      end

      result =
        new()
        |> run(:step1, transaction(:t1), compensation())
        |> run(:step2, transaction(:t2), compensation_with_retry(3))
        |> run(:step3, tx, compensation())
        |> run(:step4, transaction_with_n_errors(2, :t4), compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}
      assert_receive {:execute, :t3}

      hook_assertion.()

      # Regression test, assert that step1 is not discarded
      assert_effects([:t1, :t2, :t3, :t4])
      assert result == {:ok, :t4, %{step1: :t1, step2: :t2, step3: :t3, step4: :t4}}
    end

    test "are ignored when retry limit exceeded" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)
      test_pid = self()
      tx = transaction(:t2)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t2})
        tx.(effects_so_far, opts)
      end

      result =
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, tx, compensation())
        |> run(:step3, transaction_with_n_errors(4, :t3), compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_receive {:execute, :t2}
      assert_receive {:execute, :t2}
      assert_receive {:execute, :t2}
      refute_receive {:execute, :t2}

      assert_no_effects()
      assert result == {:error, :t3}

      hook_assertion.()
    end

    test "are ignored when compensation aborted the sage" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)
      test_pid = self()
      tx = transaction(:t2)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t2})
        tx.(effects_so_far, opts)
      end

      result =
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, tx, compensation())
        |> run(:step3, transaction_with_n_errors(1, :t3), compensation_with_abort())
        |> finally(hook)
        |> execute(a: :b)

      assert_receive {:execute, :t2}
      refute_receive {:execute, :t2}

      assert_no_effects()
      assert result == {:error, :t3}

      hook_assertion.()
    end

    test "are ignored when transaction aborted the sage" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)
      test_pid = self()
      tx = transaction(:t2)

      tx = fn effects_so_far, opts ->
        send(test_pid, {:execute, :t2})
        tx.(effects_so_far, opts)
      end

      result =
        new()
        |> run(:step1, transaction(:t1), compensation_with_retry(3))
        |> run(:step2, tx, compensation())
        |> run(:step3, transaction_with_abort(:t3), compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_receive {:execute, :t2}
      refute_receive {:execute, :t2}

      assert_no_effects()
      assert result == {:error, :t3}
      hook_assertion.()
    end
  end

  describe "circuit breaker" do
    test "response is used as transaction effect" do
      {hook, hook_assertion} = final_hook_with_assertion(:ok, a: :b)

      result =
        new()
        |> run(:step1, transaction_with_error(:t1), compensation_with_circuit_breaker(:t1_defaults))
        |> finally(hook)
        |> execute(a: :b)

      assert_no_effects()
      assert result == {:ok, :t1_defaults, %{step1: :t1_defaults}}
      hook_assertion.()
    end

    test "ignored when returned from compensation which is not responsible for failed transaction" do
      {hook, hook_assertion} = final_hook_with_assertion(:error, a: :b)

      result =
        new()
        |> run(:step1, transaction(:t1), compensation_with_circuit_breaker(:t1_defaults))
        |> run(:step2, transaction_with_error(:t2), compensation())
        |> finally(hook)
        |> execute(a: :b)

      assert_no_effects()
      hook_assertion.()
      assert result == {:error, :t2}
    end
  end

  test "executor calls are purged from stacktraces" do
    sage =
      new()
      |> run(:step1, transaction(:t1), compensation_with_retry(3))
      |> run(:step2, transaction(:t2), compensation())
      |> run_async(:step3, transaction(:t3), not_strict_compensation())
      |> run_async(:step4, transaction(:t4), not_strict_compensation())
      |> run(:step5, transaction_with_exception(:t5), compensation(:t5))

    stacktrace =
      try do
        execute(sage)
      rescue
        _exception -> System.stacktrace()
      end

    assert [
             {Sage.Fixtures, _transaction_function, _transaction_function_arity, _transaction_function_macro_env},
             {Sage.Executor, :execute, _executor_arity, _executor_macro_env},
             {Sage.ExecutorTest, _test_function, _test_function_arity, _test_function_macro_env} | _rest
           ] = stacktrace
  end

  def do_send(msg, _opts, pid), do: send(pid, msg)

  def mfa_transaction(effects_so_far, opts, cb), do: cb.(effects_so_far, opts)

  def mfa_compensation(effect_to_compensate, effects_so_far, opts, cb),
    do: cb.(effect_to_compensate, effects_so_far, opts)

  def final_hook_with_raise(status, _opts, test_pid) do
    send(test_pid, status)
    raise "Final mfa hook raised an exception"
  end

  def final_hook_with_throw(status, _opts, test_pid) do
    send(test_pid, status)
    raise "Final mfa hook threw an error"
  end

  def final_hook_with_exit(status, _opts, test_pid) do
    send(test_pid, status)
    raise "Final mfa hook exited"
  end
end
