defmodule Finch.HTTP2.RequestStreamTest do
  use ExUnit.Case, async: true

  alias Finch.HTTP2.RequestStream

  describe "next_chunk/2" do
    test "sends a body that fits into the window in one chunk" do
      stream = RequestStream.new("hello")

      assert {%RequestStream{status: :done}, chunks} = RequestStream.next_chunk(stream, 10)
      assert IO.iodata_to_binary(chunks) == "hello"
    end

    test "splits a streamed body across windows" do
      body = {:stream, ["aaaa", "bbbb", "cccc"]}
      stream = RequestStream.new(body)

      assert {%RequestStream{status: :streaming} = stream, chunks} =
               RequestStream.next_chunk(stream, 6)

      assert IO.iodata_to_binary(chunks) == "aaaabb"

      assert {%RequestStream{status: :streaming} = stream, chunks} =
               RequestStream.next_chunk(stream, 5)

      assert IO.iodata_to_binary(chunks) == "bbccc"

      assert {%RequestStream{status: :done}, chunks} = RequestStream.next_chunk(stream, 5)
      assert IO.iodata_to_binary(chunks) == "c"
    end

    test "handles enumerables that halt with more data than fits into the window" do
      # Stream.take/2 does not propagate the reducer's :suspend on its final
      # element, so the stream halts with an element that exceeds the window.
      body = {:stream, Stream.repeatedly(fn -> "xxxxxxxxxx" end) |> Stream.take(2)}
      stream = RequestStream.new(body)

      assert {%RequestStream{status: :streaming} = stream, chunks} =
               RequestStream.next_chunk(stream, 15)

      assert IO.iodata_to_binary(chunks) == String.duplicate("x", 15)

      assert {%RequestStream{status: :done}, chunks} = RequestStream.next_chunk(stream, 15)
      assert IO.iodata_to_binary(chunks) == String.duplicate("x", 5)
    end

    test "drains a halted overflow across several small windows" do
      body = {:stream, Stream.repeatedly(fn -> "xxxxxxxxxx" end) |> Stream.take(2)}
      stream = RequestStream.new(body)

      {stream, chunks} = RequestStream.next_chunk(stream, 12)
      assert IO.iodata_to_binary(chunks) == String.duplicate("x", 12)

      {stream, chunks} = RequestStream.next_chunk(stream, 3)
      assert stream.status == :streaming
      assert IO.iodata_to_binary(chunks) == "xxx"

      {stream, chunks} = RequestStream.next_chunk(stream, 100)
      assert stream.status == :done
      assert IO.iodata_to_binary(chunks) == String.duplicate("x", 5)
    end
  end
end
