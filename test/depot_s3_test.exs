defmodule DepotS3Test do
  use ExUnit.Case

  defmacrop assert_in_list(list, match) do
    quote do
      assert Enum.any?(unquote(list), &match?(unquote(match), &1))
    end
  end

  setup do
    config = DepotS3.Minio.config()
    DepotS3.Minio.clean_bucket("default")
    DepotS3.Minio.recreate_bucket("default")

    on_exit(fn ->
      DepotS3.Minio.clean_bucket("default")
    end)

    {:ok, config: config, bucket: "default"}
  end

  test "user can write to filesystem", %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")

    assert :ok = Depot.write(filesystem, "test.txt", "Hello World")
  end

  test "user can check if files exist on a filesystem", %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")

    :ok = Depot.write(filesystem, "test.txt", "Hello World")

    assert {:ok, :exists} = Depot.file_exists(filesystem, "test.txt")
    assert {:ok, :missing} = Depot.file_exists(filesystem, "not-test.txt")
  end

  test "user can read from filesystem", %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")

    :ok = Depot.write(filesystem, "test.txt", "Hello World")

    assert {:ok, "Hello World"} = Depot.read(filesystem, "test.txt")
  end

  test "user can delete from filesystem", %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")

    :ok = Depot.write(filesystem, "test.txt", "Hello World")
    :ok = Depot.delete(filesystem, "test.txt")

    assert {:error, _} = Depot.read(filesystem, "test.txt")
  end

  test "user can move files", %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")

    :ok = Depot.write(filesystem, "test.txt", "Hello World")
    :ok = Depot.move(filesystem, "test.txt", "not-test.txt")

    assert {:error, _} = Depot.read(filesystem, "test.txt")
    assert {:ok, "Hello World"} = Depot.read(filesystem, "not-test.txt")
  end

  test "user can copy files", %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")

    :ok = Depot.write(filesystem, "test.txt", "Hello World")
    :ok = Depot.copy(filesystem, "test.txt", "not-test.txt")

    assert {:ok, "Hello World"} = Depot.read(filesystem, "test.txt")
    assert {:ok, "Hello World"} = Depot.read(filesystem, "not-test.txt")
  end

  test "user can list files", %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")

    :ok = Depot.write(filesystem, "test.txt", "Hello World")
    :ok = Depot.write(filesystem, "test-1.txt", "Hello World")

    {:ok, list} = Depot.list_contents(filesystem, ".")

    assert length(list) == 2
    assert_in_list(list, %Depot.Stat.File{name: "test.txt"})
    assert_in_list(list, %Depot.Stat.File{name: "test-1.txt"})
  end
end
