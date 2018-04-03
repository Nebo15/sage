defimpl Inspect, for: Sage do
  import Inspect.Algebra

  @tx_symbol "->"
  @cmp_symbol "<-"
  @lock_symbol "l-"
  @unlock_symbol "u-"
  @tx_args [:effects_so_far, :opts]
  @cmp_args [:effect_to_compensate, :name_and_reason, :opts]
  @final_hook_args [:name, :state]

  def inspect(sage, opts) do
    list = to_list(sage)
    left = concat(["#Sage", format_compensation_error_handler(sage.on_compensation_error), "<"])
    surround_many(left, list, ">", opts, fn str, _ -> str end)
  end

  defp to_list(sage) do
    stages = sage.stages |> Enum.reverse() |> Enum.map(&format_stage/1)
    final_hooks = Enum.map(sage.final_hooks, &concat("finally: ", format_final_hook(&1)))
    Enum.concat([stages, final_hooks])
  end

  defp format_stage({name, operation}) do
    name = "#{Atom.to_string(name)}: "
    group(concat([name, nest(build_operation(operation), String.length(name))]))
  end

  defp build_operation({:run_async, transaction, compensation, tx_opts}) do
    tx = concat([format_transaction_callback(transaction), " (async)", format_transaction_opts(tx_opts)])
    cmp = format_compensation_callback(compensation)
    glue_operation(tx, cmp)
  end

  defp build_operation({:run, transaction, compensation, []}) do
    tx = concat([format_transaction_callback(transaction)])
    cmp = format_compensation_callback(compensation)
    glue_operation(tx, cmp)
  end

  defp build_operation({:lock, lock_cb, unlock_cb, []}) do
    l = format_lock_callback(lock_cb)
    ul = format_unlock_callback(unlock_cb)
    glue_operation(l, ul)
  end

  defp glue_operation(tx, ""), do: tx
  defp glue_operation(tx, cmp), do: glue(tx, cmp)

  defp format_compensation_error_handler(:raise), do: ""
  defp format_compensation_error_handler(handler), do: concat(["(with ", Kernel.inspect(handler), ")"])

  defp format_transaction_opts([]), do: ""
  defp format_transaction_opts(tx_opts), do: concat([" ", Kernel.inspect(tx_opts)])

  defp format_transaction_callback(callback), do: concat([@tx_symbol, " ", format_callback(callback, @tx_args)])

  defp format_compensation_callback(:noop), do: ""
  defp format_compensation_callback(callback), do: concat([@cmp_symbol, " ", format_callback(callback, @cmp_args)])

  defp format_lock_callback(callback), do: concat([@lock_symbol, " ", format_callback(callback, @tx_args)])

  defp format_unlock_callback(callback), do: concat([@unlock_symbol, " ", format_callback(callback, @tx_args)])

  defp format_final_hook(callback), do: format_callback(callback, @final_hook_args)

  defp format_callback({module, function, args}, default_args) do
    concat([Kernel.inspect(module), ".", Kernel.to_string(function), maybe_expand_args(args, default_args)])
  end

  defp format_callback(function, _default_args) do
    Kernel.inspect(function)
  end

  defp maybe_expand_args([], default_args) do
    "/#{to_string(length(default_args))}"
  end

  defp maybe_expand_args(args, default_args) do
    args = Enum.map(args, &Kernel.inspect/1)
    concat(["(", Enum.join(default_args ++ args, ", "), ")"])
  end
end
