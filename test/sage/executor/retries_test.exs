defmodule Sage.Executor.RetriesTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  import Sage.Executor.Retries

  describe "retry_with_backoff?/2" do
    test "limits retry count" do
      assert retry_with_backoff?(1, retry_limit: 2)
      refute retry_with_backoff?(2, retry_limit: 2)
      refute retry_with_backoff?(1, retry_limit: 0)
      refute retry_with_backoff?(1, retry_limit: -5)
    end

    test "does not apply backoff by default" do
      assert retry_execution_time(1, retry_limit: 2) < 10
    end

    test "applies backoff" do
      assert_in_delta retry_execution_time(1, retry_limit: 2, base_backoff: 10, enable_jitter: false), 20, 5
    end

    test "logs message on invalid options" do
      message_prefix = "Ignoring retry backoff options, expected base_backoff and max_backoff to be integer and >= 1, "

      fun = fn -> assert retry_with_backoff?(1, retry_limit: 5, base_backoff: -1) end
      assert capture_log(fun) =~ message_prefix <> "got: base_backoff: -1, max_backoff: 5000"

      fun = fn -> assert retry_with_backoff?(1, retry_limit: 5, base_backoff: :not_integer) end
      assert capture_log(fun) =~ message_prefix <> "got: base_backoff: :not_integer, max_backoff: 5000"

      fun = fn -> assert retry_with_backoff?(1, retry_limit: 5, base_backoff: 10, max_backoff: -1) end
      assert capture_log(fun) =~ message_prefix <> "got: base_backoff: 10, max_backoff: -1"

      fun = fn -> assert retry_with_backoff?(1, retry_limit: 5, base_backoff: 10, max_backoff: :not_integer) end
      assert capture_log(fun) =~ message_prefix <> "got: base_backoff: 10, max_backoff: :not_integer"
    end
  end

  describe "get_backoff/4" do
    test "returns 0 when base is nil" do
      assert get_backoff(1, nil, 5_000, false) == 0
    end

    test "applies exponential backoff" do
      assert get_backoff(1, 10, 5_000, false) == 20
      assert get_backoff(2, 10, 5_000, false) == 400
      assert get_backoff(3, 10, 5_000, false) == 5000
      assert get_backoff(4, 10, 5_000, false) == 5000

      assert get_backoff(1, 7, 5_000, false) == 14
      assert get_backoff(2, 7, 5_000, false) == 196
      assert get_backoff(3, 7, 5_000, false) == 2744
      assert get_backoff(4, 7, 5_000, false) == 5000
    end

    test "applies exponential backoff with jitter" do
      assert get_backoff(1, 10, 5_000, true) in 0..20
      assert get_backoff(2, 10, 5_000, true) in 0..400
      assert get_backoff(3, 10, 5_000, true) in 0..5000
      assert get_backoff(4, 10, 5_000, true) in 0..5000

      assert get_backoff(1, 7, 5_000, true) in 0..14
      assert get_backoff(2, 7, 5_000, true) in 0..196
      assert get_backoff(3, 7, 5_000, true) in 0..2744
      assert get_backoff(4, 7, 5_000, true) in 0..5000
    end
  end

  defp retry_execution_time(count, opts) do
    start = System.monotonic_time()
    retry? = retry_with_backoff?(count, opts)
    stop = System.monotonic_time()
    assert retry?
    div(System.convert_time_unit(stop - start, :native, :micro_seconds), 100) / 10
  end
end
