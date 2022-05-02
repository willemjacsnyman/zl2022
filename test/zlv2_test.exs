defmodule ZLv2Test do
  use ExUnit.Case
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

  defp type_of(r) do
    (is_list(r) and :list) ||
      (is_map(r) and :map) ||
      (is_boolean(r) and :boolean) ||
      (is_binary(r) and :binary) ||
      (is_integer(r) and :integer) ||
      (is_float(r) and :float) ||
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

      assert %{"a nil list that has been instantiated" => [nil, []]} == compare(stream, dataa, datab)
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
end
