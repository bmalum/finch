defmodule Finch.HTTP2.RequestStream do
  @moduledoc false

  defstruct [:body, :status, :buffer, :continuation]

  def new(body) do
    enumerable =
      case body do
        {:stream, stream} -> Stream.map(stream, &with_byte_size/1)
        nil -> [with_byte_size("")]
        io_data -> [with_byte_size(io_data)]
      end

    reducer = &reduce_with_suspend/2

    %__MODULE__{
      body: body,
      status: if(body == nil, do: :done, else: :streaming),
      buffer: <<>>,
      continuation: &Enumerable.reduce(enumerable, &1, reducer)
    }
  end

  defp with_byte_size(binary) when is_binary(binary), do: {binary, byte_size(binary)}
  defp with_byte_size(io_data), do: io_data |> IO.iodata_to_binary() |> with_byte_size()

  # The accumulator always holds `{message, size}` pairs, regardless of whether we
  # continue or suspend. Some enumerables (e.g. `Stream.take/2` on its final
  # element) swallow the `:suspend` returned by the reducer and halt with the
  # accumulator instead, so the `:done`/`:halted` branch below must be able to
  # read the same shape.
  defp reduce_with_suspend(
         {message, message_size},
         {message_buffer, message_buffer_size, window}
       ) do
    message_buffer = [{message, message_size} | message_buffer]
    message_buffer_size = message_size + message_buffer_size
    acc = {message_buffer, message_buffer_size, window}

    if message_buffer_size > window do
      {:suspend, acc}
    else
      {:cont, acc}
    end
  end

  # gets the next chunk of data that will fit into the given window size
  def next_chunk(request, window)

  # when the buffer is empty, continue reducing the stream
  def next_chunk(%__MODULE__{buffer: <<>>} = request, window) do
    continue_reduce(request, {[], 0, window})
  end

  def next_chunk(%__MODULE__{buffer: buffer} = request, window) do
    case buffer do
      <<bytes_to_send::binary-size(^window), rest::binary>> ->
        # when the buffer contains more bytes than a window, send as much of the
        # buffer as we can
        {put_in(request.buffer, rest), bytes_to_send}

      _ ->
        # when the buffer can fit in the windows, continue reducing using the buffer
        # as the accumulator
        continue_reduce(request, {[with_byte_size(buffer)], byte_size(buffer), window})
    end
  end

  defp continue_reduce(request, acc) do
    case request.continuation.({:cont, acc}) do
      {finished, {messages, total_size, window_size}}
      when finished in [:done, :halted] and total_size <= window_size ->
        {put_in(request.status, :done), messages_to_chunks(messages)}

      {finished, {messages, _total_size, window_size}} when finished in [:done, :halted] ->
        # The enumerable finished but handed us more bytes than fit into the
        # window (see `reduce_with_suspend/2`). Send what fits now and keep the
        # rest buffered; there is nothing left to reduce, so the next call only
        # has to drain the buffer.
        <<fittable_binary::binary-size(^window_size), overload_binary::binary>> =
          messages |> messages_to_chunks() |> IO.iodata_to_binary()

        request = %{request | continuation: &finished_continuation/1, buffer: overload_binary}

        {request, [fittable_binary]}

      {:suspended,
       {[{overload_message, overload_message_size} | messages_that_fit], total_size, window_size},
       next_continuation} ->
        fittable_size = window_size - (total_size - overload_message_size)

        <<fittable_binary::binary-size(^fittable_size), overload_binary::binary>> =
          overload_message

        request = %{request | continuation: next_continuation, buffer: overload_binary}

        {request, messages_to_chunks([{fittable_binary, fittable_size} | messages_that_fit])}
    end
  end

  defp messages_to_chunks(messages) do
    messages |> Enum.reverse() |> Enum.map(fn {message, _size} -> message end)
  end

  defp finished_continuation({:cont, acc}), do: {:done, acc}
end
