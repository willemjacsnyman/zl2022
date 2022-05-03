defmodule ZLv2Test do
  use ExUnit.Case, async: true
  # doctest ZL?

  describe "hash/2" do
    test "can hash a string" do
      stream = "stream"
      data = "A string value"
      assert %{"#" => stream, stream => data} == hash(stream, data)
    end

    test "can hash a large string" do
      stream = "A large string"
      data = File.read!("test/lorem_ipsum.txt")
      assert %{"#" => stream, stream => data} == hash(stream, data)
    end

    test "can hash a list" do
      stream = "stream"
      data = []
      assert %{"#" => stream, stream => data} == hash(stream, data)
      data = ["a"]
      assert %{"#" => "stream", "stream" => ["stream/0"], "stream/0" => "a"} == hash(stream, data)
    end

    test "can hash a list with multiple items" do
      stream = "a list with multple items"
      data = ["1", "2", "3"]

      assert %{
               "#" => "a list with multple items",
               "a list with multple items" => [
                 "a list with multple items/0",
                 "a list with multple items/1",
                 "a list with multple items/2"
               ],
               "a list with multple items/0" => "1",
               "a list with multple items/1" => "2",
               "a list with multple items/2" => "3"
             } == hash(stream, data)
    end

    test "can hash a list with lists for items" do
      stream = "a list with lists"
      data = [["a1", "a2"], ["b1", "b2"]]

      assert %{
               "#" => "a list with lists",
               "a list with lists" => ["a list with lists/0", "a list with lists/1"],
               "a list with lists/0" => ["a list with lists/0/0", "a list with lists/0/1"],
               "a list with lists/0/0" => "a1",
               "a list with lists/0/1" => "a2",
               "a list with lists/1" => ["a list with lists/1/0", "a list with lists/1/1"],
               "a list with lists/1/0" => "b1",
               "a list with lists/1/1" => "b2"
             } == hash(stream, data)
    end

    test "can hash an empty map" do
      stream = "an empty map"
      data = %{}

      assert %{"#" => "an empty map", "an empty map" => %{}} == hash(stream, data)
    end

    test "can hash map with string value" do
      stream = "map with string value"
      data = %{"a" => "b"}

      assert %{
               "#" => "map with string value",
               "map with string value" => %{"a" => "map with string value/a"},
               "map with string value/a" => "b"
             } == hash(stream, data)
    end

    test "can hash map with list value" do
      stream = "map with list value"
      data = %{"a" => ["b"]}

      assert %{
               "#" => "map with list value",
               "map with list value" => %{"a" => "map with list value/a"},
               "map with list value/a" => ["map with list value/a/0"],
               "map with list value/a/0" => "b"
             } == hash(stream, data)
    end

    test "can hash map with map value" do
      stream = "map with map value"
      data = %{"a" => %{}}

      assert %{
               "#" => "map with map value",
               "map with map value" => %{"a" => "map with map value/a"},
               "map with map value/a" => %{}
             } == hash(stream, data)

      data = %{"a" => %{"b" => "c"}}

      assert %{
               "#" => "map with map value",
               "map with map value" => %{"a" => "map with map value/a"},
               "map with map value/a" => %{"b" => "map with map value/a/b"},
               "map with map value/a/b" => "c"
             } == hash(stream, data)
    end

    test "can hash list with map values" do
      stream = "list with map value"
      data = [%{}]

      assert %{
               "#" => "list with map value",
               "list with map value" => ["list with map value/0"],
               "list with map value/0" => %{}
             } == hash(stream, data)

      data = [%{"a" => "b1"}, %{"a" => "b2"}]

      assert %{
               "#" => "list with map value",
               "list with map value" => ["list with map value/0", "list with map value/1"],
               "list with map value/0" => %{"a" => "list with map value/0/a"},
               "list with map value/0/a" => "b1",
               "list with map value/1" => %{"a" => "list with map value/1/a"},
               "list with map value/1/a" => "b2"
             } == hash(stream, data)
    end

    test "can hash a map with multiple key value pairs" do
      stream = "multiple key value paired map"
      data = %{"a" => "b", "c" => "d", "e" => "f"}

      assert %{
               "#" => "multiple key value paired map",
               "multiple key value paired map" => %{
                 "a" => "multiple key value paired map/a",
                 "c" => "multiple key value paired map/c",
                 "e" => "multiple key value paired map/e"
               },
               "multiple key value paired map/a" => "b",
               "multiple key value paired map/c" => "d",
               "multiple key value paired map/e" => "f"
             } == hash(stream, data)
    end
  end

  def hash(stream, data) do
    hash(stream, data, type_of(data), %{})
    |> Map.put_new("#", stream)
  end

  defp hash(stream, data, :map, hashmap) do
    hash(stream, Map.to_list(data), :map, hashmap, [])
  end

  defp hash(stream, data, :list, hashmap) do
    hash(stream, data, :list, hashmap, [])
  end

  defp hash(stream, data, _, hashmap) do
    Map.put_new(hashmap, stream, data)
  end

  defp hash(stream, [{k, v} | data], :map, hashmap, completed) do
    substream = "#{stream}/#{k}"
    hashmap = hash(substream, v, type_of(v), hashmap)
    hash(stream, data, :map, hashmap, [{k, substream} | completed])
  end

  defp hash(stream, [], :map, hashmap, completed) do
    {_, completed} =
      Enum.map_reduce(completed, %{}, fn {k, v}, acc -> {{k, v}, Map.put_new(acc, k, v)} end)

    Map.put_new(hashmap, stream, completed)
  end

  defp hash(stream, [item | data], :list, hashmap, completed) do
    index = length(completed)
    substream = "#{stream}/#{index}"
    hashmap = hash(substream, item, type_of(item), hashmap)
    hash(stream, data, :list, hashmap, [substream | completed])
  end

  defp hash(stream, [], :list, hashmap, completed) do
    Map.put_new(hashmap, stream, Enum.reverse(completed))
  end

  describe "type_of/1" do
    test "lists" do
      assert :list == type_of([])
      assert :list == type_of([1])
      assert :list == type_of([1, "2"])
      assert :list == type_of([1, "2", ["3"]])
    end

    test "maps" do
      assert :map == type_of(%{})
      assert :map == type_of(%{"a" => 1})
      assert :map == type_of(%{"a" => 1, "b" => []})
      assert :map == type_of(%{"a" => 1, "b" => [2], "c" => %{"d" => nil}})
    end

    test "booleans" do
      assert :boolean == type_of(true)
      assert :boolean == type_of(false)
    end

    test "binary" do
      assert :binary == type_of("")
      assert :binary == type_of("  ")
      assert :binary == type_of("some text")
      assert :binary == type_of("false")
      assert :binary == type_of("[]")
    end

    test "integer" do
      assert :integer == type_of(0)
      assert :integer == type_of(1)
      assert :integer == type_of(-1)

      assert :integer ==
               type_of(1_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000)

      assert :integer ==
               type_of(-1_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000)
    end

    test "float" do
      assert :float == type_of(0.0)
      assert :float == type_of(0.0000001)
      assert :float == type_of(1_000_000.00)
      assert :float == type_of(1_000_000.0000001)
    end
  end

  def type_of(r) do
    (is_list(r) and :list) ||
      (is_map(r) and :map) ||
      (is_boolean(r) and :boolean) ||
      (is_binary(r) and :binary) ||
      (is_integer(r) and :integer) ||
      (is_float(r) and :float) ||
      (is_nil(r) and :null) ||
      :unknown
  end

  describe "compare/3" do
    test "given two strings that are different" do
      stream = "stream"
      dataa = "A"
      datab = "B"
      assert %{"stream" => ["A", "B"]} == compare(stream, dataa, datab)
    end

    test "given a string that will be disposed" do
      stream = "stream"
      dataa = "A"
      datab = nil
      assert %{"stream" => ["A", nil]} == compare(stream, dataa, datab)
    end

    test "given a nil string that will be initialised" do
      stream = "stream"
      dataa = nil
      datab = ""
      assert %{"stream" => [nil, ""]} == compare(stream, dataa, datab)
    end

    test "given two lists that are different" do
      stream = "stream"
      dataa = ["A"]
      datab = ["B"]
      assert %{"stream/0" => ["A", "B"]} == compare(stream, dataa, datab)
    end

    test "given an empty list that has been populated" do
      stream = "a list that will be populated"
      empty_list = []
      populated_list = ["A"]

      assert %{
               "a list that will be populated" => [[], ["a list that will be populated/0"]],
               "a list that will be populated/0" => [nil, "A"]
             } ==
               compare(stream, empty_list, populated_list)
    end

    test "given a nil list that has been populated" do
      stream = "a list that will be populated"
      nil_list = nil
      populated_list = ["A"]

      assert %{
               "a list that will be populated" => [nil, ["a list that will be populated/0"]],
               "a list that will be populated/0" => [nil, "A"]
             } ==
               compare(stream, nil_list, populated_list)
    end

    test "given a nil list that has been instantiated" do
      stream = "a nil list that has been instantiated"
      dataa = nil
      datab = []

      assert %{"a nil list that has been instantiated" => [nil, []]} ==
               compare(stream, dataa, datab)
    end

    test "given a list where a change is on an indexed item" do
      stream = "stream"
      dataa = ["Indexed item"]
      datab = ["Changed indexed item"]

      assert %{"stream/0" => ["Indexed item", "Changed indexed item"]} ==
               compare(stream, dataa, datab)
    end

    test "given a list where item was added" do
      stream = "stream"
      dataa = ["An item"]
      datab = ["An item", "An item that was added"]

      assert %{
               "stream" => [["stream/0"], ["stream/0", "stream/1"]],
               "stream/1" => [nil, "An item that was added"]
             } ==
               compare(stream, dataa, datab)
    end

    test "given a list where item was removed" do
      stream = "list where item was removed"
      dataa = ["An item", "An item that will be removed"]
      datab = ["An item"]

      assert %{
               "list where item was removed" => [
                 ["list where item was removed/0", "list where item was removed/1"],
                 ["list where item was removed/0"]
               ],
               "list where item was removed/1" => ["An item that will be removed", nil]
             } ==
               compare(stream, dataa, datab)
    end

    test "given a list that was emptied" do
      stream = "list that will be emptied"
      dataa = ["An item"]
      empty_list = []

      assert %{
               "list that will be emptied" => [["list that will be emptied/0"], []],
               "list that will be emptied/0" => ["An item", nil]
             } ==
               compare(stream, dataa, empty_list)
    end

    test "given a list where an indexed item is disposed" do
      stream = "list that will be emptied"
      dataa = ["An item"]
      list_with_disposed_indexed_item = [nil]

      assert %{"list that will be emptied/0" => ["An item", nil]} ==
               compare(stream, dataa, list_with_disposed_indexed_item)
    end

    test "given a map has been initialised" do
      stream = "map that will be initialised"
      dataa = nil
      an_initialised_map = %{}

      assert %{"map that will be initialised" => [nil, %{}]} ==
               compare(stream, dataa, an_initialised_map)
    end

    test "given a map has been initialised and populated" do
      stream = "map that will be populated"
      dataa = nil
      an_initialised_map = %{"a" => "b"}

      assert %{
               "map that will be populated" => [nil, %{"a" => "map that will be populated/a"}],
               "map that will be populated/a" => [nil, "b"]
             } == compare(stream, dataa, an_initialised_map)
    end

    test "given a map where a value has been changed" do
      stream = "a map with a changed value"
      dataa = %{"a" => "b"}
      datab = %{"a" => "c"}
      assert %{"a map with a changed value/a" => ["b", "c"]} == compare(stream, dataa, datab)
    end

    test "given a map where a value has been added" do
      stream = "a map with an added value"
      dataa = %{"a" => "b"}
      datab = %{"a" => "b", "c" => "d"}

      assert %{
               "a map with an added value" => [
                 %{"a" => "a map with an added value/a"},
                 %{"a" => "a map with an added value/a", "c" => "a map with an added value/c"}
               ],
               "a map with an added value/c" => [nil, "d"]
             } == compare(stream, dataa, datab)
    end

    test "given a map where a value has been initialised as a list" do
      stream = "a map with an initialised list"
      dataa = %{}
      datab = %{"a" => []}

      assert %{
               "a map with an initialised list" => [
                 %{},
                 %{"a" => "a map with an initialised list/a"}
               ],
               "a map with an initialised list/a" => [nil, []]
             } == compare(stream, dataa, datab)
    end

    test "given a map where a value has been initialised as a populated list" do
      stream = "a map with a populated list"
      dataa = %{}
      datab = %{"a" => ["b", "c"]}

      assert %{
               "a map with a populated list" => [%{}, %{"a" => "a map with a populated list/a"}],
               "a map with a populated list/a" => [
                 nil,
                 ["a map with a populated list/a/0", "a map with a populated list/a/1"]
               ],
               "a map with a populated list/a/0" => [nil, "b"],
               "a map with a populated list/a/1" => [nil, "c"]
             } == compare(stream, dataa, datab)
    end

    test "given a map where a populated list value has been initialised has been emptied" do
      stream = "a map"
      dataa = %{"a" => ["b"]}
      datab = %{"a" => []}

      assert %{"a map/a" => [["a map/a/0"], []], "a map/a/0" => ["b", nil]} ==
               compare(stream, dataa, datab)

      diff = compare(stream, dataa, datab)

      hash(stream, dataa)
      |> Map.merge(diff)
      |> Enum.map_reduce(%{}, fn
        {k, [_old, new]}, acc -> {{k, new}, Map.put_new(acc, k, new)}
        {k, v}, acc -> {{k, v}, Map.put_new(acc, k, v)}
      end)
    end
  end

  def compare(stream, a, b) do
    hasha = hash(stream, a)
    hashb = hash(stream, b)

    {_, diffb} =
      Map.delete(hashb, "#")
      |> Enum.filter(fn {k, v} -> hasha[k] != v end)
      |> Enum.map(fn {k, v} -> {k, [hasha[k], v]} end)
      |> Enum.map_reduce(%{}, fn {k, v}, acc -> {{k, v}, Map.put_new(acc, k, v)} end)

    {_, diffa} =
      Map.delete(hasha, "#")
      |> Enum.filter(fn {k, v} -> !diffb[k] and hashb[k] != v end)
      |> Enum.map(fn {k, v} -> {k, [v, nil]} end)
      |> Enum.map_reduce(%{}, fn {k, v}, acc -> {{k, v}, Map.put_new(acc, k, v)} end)

    Map.merge(diffb, diffa)
  end

  describe "rebuild/2" do
    test "given a stream with nil, returns the nil" do
      stream = "a stream for nil"
      data = nil
      hasheddata = hash(stream, data)
      assert nil == rebuild(stream, hasheddata)
    end

    test "given a stream with a hashed binary, returns the original binary" do
      stream = "a stream for a binary"
      data = "a binary"
      hasheddata = hash(stream, data)

      assert "a binary" == rebuild(stream, hasheddata)
    end

    test "given a stream with a hashed list, returns the original list" do
      stream = "a stream for a list"
      data = []
      hasheddata = hash(stream, data)

      assert [] == rebuild(stream, hasheddata)
    end

    test "given a stream with a hashed populated list, returns the original list" do
      stream = "a stream for a populated list"
      data = ["a", "b"]
      hasheddata = hash(stream, data)
      assert ["a", "b"] == rebuild(stream, hasheddata)
    end

    test "given a stream with a hashed list with nested lists, returns the original list" do
      stream = "a stream for a list with nested lists"
      data = [["a", "b"], ["c"], []]
      hasheddata = hash(stream, data)
      assert [["a", "b"], ["c"], []] == rebuild(stream, hasheddata)
    end

    test "given a stream for an initialised map, returns the original map" do
      stream = "a stream for an initialised map"
      data = %{}
      hasheddata = hash(stream, data)
      assert %{} == rebuild(stream, hasheddata)
    end

    test "given a stream for a populated map, returns the original map" do
      stream = "a stream for a populated map"
      data = %{"a" => "b"}
      hasheddata = hash(stream, data)
      assert %{"a" => "b"} == rebuild(stream, hasheddata)
    end

    test "given a stream for a populated map with populated list values, returns the original map" do
      stream = "a stream for a populated map with populated list value"
      data = %{"a" => ["b", "c"], "d" => ["e"], "f" => []}
      hasheddata = hash(stream, data)
      assert %{"a" => ["b", "c"], "d" => ["e"], "f" => []} == rebuild(stream, hasheddata)
    end
  end

  def rebuild(stream, hash) when is_map(hash) do
    rebuild(stream, hash, hash["#"])
  end

  defp rebuild(stream, hash, streamh) when stream == streamh do
    data = hash[stream]
    rebuild(stream, hash, type_of(data))
  end

  defp rebuild(_, _, :null) do
    nil
  end

  defp rebuild(stream, hash, :binary) do
    hash[stream]
  end

  defp rebuild(stream, hash, :list) do
    hash[stream]
    |> Enum.map(fn item -> rebuild(item, hash, type_of(hash[item])) end)
  end

  defp rebuild(stream, hash, :map) do
    {_, data} =
      hash[stream]
      |> Enum.map(fn {k, v} -> {k, rebuild(v, hash, type_of(hash[v])) || v} end)
      |> Enum.map_reduce(%{}, fn {k, v}, acc -> {{k, v}, Map.put_new(acc, k, v)} end)

    data
  end

  describe "apply_diff/2" do
    test "a binary stream that has been initialised" do
      stream = "stream"
      datab = "a"

      diff = compare(stream, nil, datab)

      assert %{"stream" => "a"} == apply_diff(stream, diff)
    end

    test "a binary stream that has been disposed" do
      stream = "stream"
      data = "a"

      diff = compare(stream, data, nil)

      assert %{"stream" => nil} == apply_diff(stream, diff)
    end

    test "a binary stream that has been changed" do
      stream = "stream"
      data = "a"
      datab = "b"

      diff = compare(stream, data, datab)

      assert %{"stream" => "b"} == apply_diff(stream, diff)
    end

    test "{:error, :nodiff} is returned when there are no changes" do
      stream = "stream"
      data = "a"
      datab = "a"

      diff = compare(stream, data, datab)

      assert {:error, :nodiff} == apply_diff(stream, diff)
    end

    test "a list that has been initialised" do
      stream = "list"
      data = nil
      datab = []

      diff = compare(stream, data, datab)

      assert %{"list" => []} == apply_diff(stream, diff)
    end

    test "an initialised list that has been disposed" do
      stream = "list"
      data = []
      datab = nil

      diff = compare(stream, data, datab)

      assert %{"list" => nil} == apply_diff(stream, diff)
    end

    test "a populated list that has been disposed" do
      stream = "list"
      data = ["a"]
      datab = nil

      diff = compare(stream, data, datab)

      assert %{"list" => nil} == apply_diff(stream, diff)
    end

    test "a populated list that has been emptied" do
      stream = "list"
      data = ["a"]
      datab = []

      diff = compare(stream, data, datab)

      assert %{"list" => []} == apply_diff(stream, diff)
    end

    test "a list with that has been initialised and populated" do
      stream = "list"
      data = nil
      datab = ["a", "b"]

      diff = compare(stream, data, datab)

      assert %{"list" => ["a", "b"]} == apply_diff(stream, diff)
    end
  end

  def apply_diff(_, diff) when map_size(diff) == 0 do
    {:error, :nodiff}
  end

  def apply_diff(stream, diff) do
    apply_diff(stream, diff, diff[stream])
  end

  defp apply_diff(stream, diff, [_, v]) do
    apply_diff(stream, diff, type_of(v), v)
  end

  defp apply_diff(stream, _, :null, nil) do
    %{stream => nil}
  end

  defp apply_diff(stream, _, :binary, binary) do
    %{stream => binary}
  end

  defp apply_diff(stream, diff, :list, list) do
    apply_diff(stream, diff, :list, list, [])
  end

  defp apply_diff(stream, diff, :list, [item | list], completed) do
    item = apply_diff(item, diff)[item]
    apply_diff(stream, diff, :list, list, [item | completed])
  end

  defp apply_diff(stream, _, :list, [], completed) do
    %{stream => Enum.reverse(completed)}
  end
end
