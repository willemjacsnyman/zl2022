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

  test "merge" do
    assert merge("a", nil, nil) == {"a", nil, nil, %{}}
    assert merge("a", 1, nil) == {"a", 1, nil, %{"a" => [1, nil]}}
    assert merge("a", [1], [2]) == {"a", [1], [2], %{"a/0" => [1, 2]}}

    assert merge("a", %{"b" => "c1"}, %{"b" => "c2"}) ==
             {"a", %{"b" => "c1"}, %{"b" => "c2"}, %{"a/b" => ["c1", "c2"]}}

             assert merge("a", %{"b" => ["c1"]}, %{"b" => ["c1", "c2"]}) ==
              {"a", %{"b" => ["c1"]}, %{"b" => ["c1", "c2"]}, %{"a/b" => [["a/b/0"], ["a/b/0", "a/b/1"]], "a/b/1" => [nil, "c2"]}}

  end

  def merge(stream, dataa, datab) do
    a = deflate(stream, dataa)
    b = deflate(stream, datab)

    {_, _, changes} =
      merge(
        stream,
        a,
        b,
        a["#"] == stream and b["#"] == stream
      )

    {stream, dataa, datab, changes}
  end

  defp merge(_, _, _, false) do
    {:error, :invalid_stream}
  end

  defp merge(_, a, b, true) do
    diff(a, b)
  end

  # "a", %{"b" => "c1"}, %{"b" => "c2"}
  defp diff(a, b) do
    # [%{"a/b" => ["c1", "c2"]}]
    changesa = diff(a, b, [])

    # {[%{"a/b" => ["c1", "c2"]}], %{"a/b" => ["c1", "c2"]}}
    {_, changesb} =
      diff(b, a, [])
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

  defp diff(a, b, changes) do
    {_, acc} =
      Map.delete(a, "#")
      |> Map.to_list()
      |> Enum.map_reduce(changes, fn {k, v}, acc ->
        acc = diff(k, [a[k], b[k]], a[k] != b[k], acc)
        {{k, v}, acc}
      end)

    acc
  end

  defp diff(k, v, true, acc) do
    [%{k => v} | acc]
  end

  defp diff(_, _, false, acc) do
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
