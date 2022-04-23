defmodule ZLTest do
  use ExUnit.Case
  doctest ZL

  test "deflate/2" do
    assert %{"#" => "a", "a" => "b"} = deflate("a", "b")
    assert %{"#" => "a", "a" => nil} = deflate("a", nil)
    assert %{"#" => "a", "a" => []} = deflate("a", [])
    assert %{"#" => "a", "a" => 1} = deflate("a", 1)
    assert %{"#" => "a", "a" => true} = deflate("a", true)
    assert %{"#" => "a", "a" => ["a/0"], "a/0" => true} = deflate("a", [true])

    assert %{"#" => "a", "a" => ["a/0", "a/1"], "a/0" => true, "a/1" => 1} =
             deflate("a", [true, 1])

    assert %{"#" => "a", "a" => %{}} = deflate("a", %{})

    assert %{"#" => "a", "a" => %{"b" => "a/b", "d" => "a/d"}, "a/b" => "c", "a/d" => "e"} =
             deflate("a", %{"b" => "c", "d" => "e"})
  end

  test "inflate/1" do
    assert {"a", "b"} = deflate("a", "b") |> inflate()
    assert {"a", nil} = deflate("a", nil) |> inflate()
    assert {"a", []} = deflate("a", []) |> inflate()
    assert {"a", %{}} = deflate("a", %{}) |> inflate()
    assert {"a", 1} = deflate("a", 1) |> inflate()
    assert {"a", true} = deflate("a", true) |> inflate()
    assert {"a", ["b"]} = deflate("a", ["b"]) |> inflate()
    assert {"a", ["b", 1]} = deflate("a", ["b", 1]) |> inflate()
    assert {"a", ["b", 1, false]} = deflate("a", ["b", 1, false]) |> inflate()

    assert {"a", ["b", 1, false, %{"b" => "c"}]} =
             deflate("a", ["b", 1, false, %{"b" => "c"}]) |> inflate()

    assert {"a", %{"b" => "c"}} = deflate("a", %{"b" => "c"}) |> inflate()
    os_time = System.os_time()

    assert {"a", %{"b" => ["c", "d", 1, 2, 3, 100, 0, 0, %{1 => os_time}]}} ==
             deflate("a", %{"b" => ["c", "d", 1, 2, 3, 100, 0, 0, %{1 => os_time}]})
             |> inflate()
  end

  test "diff/3" do
    assert diff("a", nil, nil) == {"a", nil, %{}}
    assert diff("a", 1, nil) == {"a", 1, %{"a" => [1, nil]}}
    assert diff("a", [1], [2]) == {"a", [1], %{"a/0" => [1, 2]}}

    assert diff("a", %{"b" => "c1"}, %{"b" => "c2"}) ==
             {"a", %{"b" => "c1"}, %{"a/b" => ["c1", "c2"]}}

    assert diff("a", %{"b" => ["c1"]}, %{"b" => ["c1", "c2"]}) ==
             {"a", %{"b" => ["c1"]},
              %{"a/b" => [["a/b/0"], ["a/b/0", "a/b/1"]], "a/b/1" => [nil, "c2"]}}
  end

  test "change/3" do
    # assert test_change3("a", nil, 1) == 1
    # assert test_change3("a", 1, 2) == 2
    # assert test_change3("a", 2, nil) == nil
    # assert test_change3("nil_to_empty", nil, []) == []
    # assert test_change3("arr in arr", [], [[]]) == [[]]
    # assert test_change3("multi arr in arr", [], [[], []]) == [[], []]
    # assert test_change3("arr in arr in arr", [[]], [[[]]]) == [[[]]]
    # assert test_change3("arrs in arr to arr", [[], []], []) == []

    # assert test_change3("nil_to_empty_map", nil, %{}) == %{}

    # assert test_change3("empty_map_to_somekv", %{}, %{"somek" => "somev"}) == %{
    #          "somek" => "somev"
    #        }

    # assert test_change3("somekv_to_empty", %{"somek" => "somev"}, %{}) == %{}

    # assert test_change3("somekv_from_v_to_nil", %{"somek" => "somev"}, %{"somek" => nil}) == %{
    #          "somek" => nil
    #        }

    assert test_change3("somekv_from_v_to_nil", %{"somek" => "somev"}, %{"somek" => "somev2"}) ==
             %{
               "somek" => "somev2"
             }

    # assert test_change3("somekv_from_nil_to_v", %{"somek" => nil}, %{"somek" => "v"}) == %{
    #          "somek" => "v"
    #        }

    # assert test_change3("map_in_map", %{"m" => nil}, %{"m" => %{"m" => "v"}}) == %{
    #          "m" => %{"m" => "v"}
    #        }

    # assert test_change3("map_in_list", [%{"m" => %{}}], [%{"m" => %{"v" => [[[]]]}}]) ==
    #          [%{"m" => %{"v" => [[[]]]}}]
  end

  defp test_change3(stream, old, new) do
    {stream, old, changes} = diff(stream, old, new)
    change(stream, old, changes)
  end

  def change(stream, record, changes) do
    IO.inspect("")

    deflated_stream_record =
      deflate(stream, record)
      |> IO.inspect(label: :deflated_stream_record)

    changes
    |> IO.inspect(label: :changes)

    deflated_and_merged =
      deflated_stream_record
      |> Map.merge(changes)
      |> IO.inspect(label: :deflated_and_merged)

    {_, deflated_merged} =
      deflated_and_merged
      |> Enum.map_reduce(%{}, fn {k, v}, acc ->
        {{k, v}, Map.put_new(acc, k, map_reduce_merge(v))}
      end)

    {^stream, inflated} =
      deflated_merged
      |> inflate()

    inflated
  end

  # defp type_of_change(nil, v) when not is_nil(v) do
  #   :+
  # end

  # defp type_of_change(r, nil) when not is_nil(r) do
  #   :-
  # end

  # defp type_of_change(r, v) when r != v do
  #   type_of_r = type_of_arg(r)
  #   type_of_v = type_of_arg(v)
  #   type_changed = type_of_r != type_of_v
  #   {:=, type_of_r, type_of_v}
  # end

  # defp type_of_arg(r) do
  #   (is_list(r) and :list) ||
  #     (is_map(r) and :map) ||
  #     (is_boolean(r) and :boolean) ||
  #     (is_binary(r) and :binary) ||
  #     (is_integer(r) and :integer) ||
  #     (is_float(r) and :float) ||
  #     (is_number(r) and :number) ||
  #     :unknown
  # end

  defp map_reduce_merge([_, new]) do
    new
  end

  defp map_reduce_merge(current) do
    current
  end

  def diff(stream, a, b) do
    deflateda = deflate(stream, a)
    deflatedb = deflate(stream, b)

    {_, _, diff} =
      diff(
        stream,
        deflateda,
        deflatedb,
        deflateda["#"] == stream and deflatedb["#"] == stream
      )

    {stream, a, diff}
  end

  defp diff(_, _, _, false) do
    {:error, :invalid_stream}
  end

  defp diff(_, a, b, true) do
    changesa = changes(a, b)
    changesb = changes(b, a)

    {_, changesb} =
      changesb
      # swap b's changes around, this will yield the '+' data.
      |> Enum.map(fn m ->
        k = Map.keys(m) |> List.first()
        [o, v] = m[k]
        Map.put(m, k, [v, o])
      end)
      |> Enum.map_reduce(%{}, fn a, acc ->
        {a, Map.merge(acc, a)}
      end)

    # {[%{"a/b" => ["c1", "c2"]}], %{"a/b" => ["c1", "c2"]}}
    {_, changesa} =
      changesa
      |> Enum.map_reduce(%{}, fn a, acc ->
        {a, Map.merge(acc, a)}
      end)

    # %{"a/b" => ["c1", "c2"]}
    changes = Map.merge(changesb, changesa)

    {inflate(a), inflate(b), changes}
  end

  defp changes(a, b) do
    {_, acc} =
      Map.delete(a, "#")
      |> Map.to_list()
      |> Enum.map_reduce([], fn {k, v}, acc ->
        acc = changes(k, [a[k], b[k]], a[k] != b[k], acc)
        {{k, v}, acc}
      end)

    acc
  end

  defp changes(k, v, true, acc) do
    [%{k => v} | acc]
  end

  defp changes(_, _, false, acc) do
    acc
  end

  def inflate(hashmap) do
    stream = hashmap["#"]
    inflate(hashmap, stream, :stream)
  end

  defp inflate(hashmap, stream, :stream) do
    data = hashmap[stream]
    inflate(hashmap, {stream, data}, :data)
  end

  defp inflate(hashmap, {stream, data}, :data) when is_map(data) do
    {_, data} =
      Enum.map(data, fn {k, v} ->
        {_, x} = inflate(hashmap, v, :stream)
        {k, x}
      end)
      |> Enum.map_reduce(%{}, fn {k, v}, acc -> {:ok, Map.put_new(acc, k, v)} end)

    {stream, data}
  end

  defp inflate(hashmap, {stream, data}, :data) when is_list(data) do
    data =
      Enum.map(data, fn v ->
        {_, x} = inflate(hashmap, v, :stream)
        x
      end)

    inflate(hashmap, {stream, data}, :list, [])
  end

  defp inflate(_, {stream, data}, :data) do
    {stream, data}
  end

  defp inflate(hashmap, {stream, [data | more_data]}, :list, completed) do
    inflate(hashmap, {stream, more_data}, :list, completed ++ [data])
  end

  defp inflate(_, {stream, []}, :list, data) do
    {stream, data}
  end

  def deflate(stream, data) when is_binary(stream) do
    deflate(stream, data, %{})
    |> Map.put_new("#", stream)
  end

  def deflate(_, _) do
    {:error, :invalid_stream}
  end

  defp deflate(stream, data, hashmap) when is_map(data) do
    {data, hashmap} =
      data
      |> Map.to_list()
      |> Enum.map_reduce(hashmap, fn {index, data}, acc ->
        new_stream = "#{stream}/#{index}"
        acc = deflate(new_stream, data, acc)
        {{index, new_stream}, acc}
      end)

    {_, data} =
      data
      |> Enum.map_reduce(%{}, fn {k, v}, acc -> {:ok, Map.put_new(acc, k, v)} end)

    Map.put_new(hashmap, stream, data)
  end

  defp deflate(stream, data, hashmap) when is_list(data) do
    deflate(stream, data, :list, hashmap, 0, [])
  end

  defp deflate(stream, data, hashmap) do
    Map.put_new(hashmap, stream, data)
  end

  defp deflate(stream, [data | more_data], :list, hashmap, index, completed) do
    new_stream = "#{stream}/#{index}"
    hashmap = deflate(new_stream, data, hashmap)
    deflate(stream, more_data, :list, hashmap, index + 1, [new_stream | completed])
  end

  defp deflate(stream, [], :list, hashmap, _, completed) do
    completed = completed |> Enum.reverse()
    Map.put_new(hashmap, stream, completed)
  end
end
