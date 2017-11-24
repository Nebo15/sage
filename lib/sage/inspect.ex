defimpl Inspect, for: Sage do
  import Inspect.Algebra

  def inspect(sage, opts) do
    %{
      operations: operations,
      final_hooks: final_hooks,
      on_compensation_error: on_compensation_error
    } = sage

    final_hooks = for hook <- final_hooks, do: {:finally, hook}

    on_compensation_error =
      if on_compensation_error == :raise do
        "!"
      else
        concat(["(with ", to_doc(on_compensation_error, opts), ")"])
      end

    operations = Enum.reverse(operations) ++ final_hooks

    surround_many(concat(["#Sage", on_compensation_error ,"<"]), operations , ">", opts, fn
      {name, {kind, transaction, compensation, tx_opts}}, opts ->
        kind = if kind == :run_async, do: " (async)", else: ""

        tx = concat([
          format_transaction_callback(transaction, opts),
          kind,
          format_transaction_opts(tx_opts),
        ])

        cmp = format_compensation_callback(compensation, opts)

        concat([
          Atom.to_string(name),
          ":",
          nest(line(tx, cmp), 6)
        ])

      {:finally, hook}, opts ->
        concat([
          "finally: ",
          format_final_hook(hook, opts)
        ])
    end)
  end

  def format_transaction_opts([]), do: ""
  def format_transaction_opts(tx_opts), do: concat([" ", Kernel.inspect(tx_opts)])

  def format_transaction_callback({m, f, []}, _opts) do
    "  -> #{Kernel.inspect(m)}.#{Kernel.to_string(f)}/2"
  end
  def format_transaction_callback({m, f, a}, _opts) do
    args = a |> Enum.map(&Kernel.inspect/1) |> Enum.join(", ")
    "  -> #{Kernel.inspect(m)}.#{Kernel.to_string(f)}(effects_so_far, opts, #{args})"
  end
  def format_transaction_callback(cb, opts) do
    concat("  -> ", to_doc(cb, opts))
  end

  def format_compensation_callback({m, f, []}, _opts) do
    "  <- #{Kernel.inspect(m)}.#{Kernel.to_string(f)}/3"
  end
  def format_compensation_callback({m, f, a}, _opts) do
    args = a |> Enum.map(&Kernel.inspect/1) |> Enum.join(", ")
    "  <- #{Kernel.inspect(m)}.#{Kernel.to_string(f)}(effect_to_compensate, name_and_reason, opts, #{args})"
  end
  def format_compensation_callback(:noop, _opts) do
    ""
  end
  def format_compensation_callback(cb, opts) do
    concat("  <- ", to_doc(cb, opts))
  end

  def format_final_hook({m, f, []}, _opts) do
    "#{Kernel.inspect(m)}.#{Kernel.to_string(f)}/2"
  end
  def format_final_hook({m, f, a}, _opts) do
    args = a |> Enum.map(&Kernel.inspect/1) |> Enum.join(", ")
    "#{Kernel.inspect(m)}.#{Kernel.to_string(f)}(name, state, #{args})"
  end
  def format_final_hook(hook, opts) do
    to_doc(hook, opts)
  end
end
