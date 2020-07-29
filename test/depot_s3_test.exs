defmodule DepotS3Test do
  use ExUnit.Case
  import Depot.AdapterTest

  setup do
    config = DepotS3.Minio.config()
    DepotS3.Minio.clean_bucket("default")
    DepotS3.Minio.recreate_bucket("default")

    on_exit(fn ->
      DepotS3.Minio.clean_bucket("default")
    end)

    {:ok, config: config, bucket: "default"}
  end

  adapter_test %{config: config} do
    filesystem = DepotS3.configure(config: config, bucket: "default")
    {:ok, filesystem: filesystem}
  end

  # Keep here until AdapterTest does include tests for read_stream
  test "stream success", %{config: config} do
    {_, config} = DepotS3.configure(config: config, bucket: "default")

    DepotS3.write(config, "test.txt", "Hello World")

    assert {:ok, stream} = DepotS3.read_stream(config, "test.txt", chunk_size: 2)

    assert Enum.into(stream, <<>>) == "Hello World"
  end
end
