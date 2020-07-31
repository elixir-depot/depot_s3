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

  describe "cross bucket" do
    setup %{config: config} do
      config_b = DepotS3.Minio.config()
      DepotS3.Minio.clean_bucket("secondary")
      DepotS3.Minio.recreate_bucket("secondary")

      on_exit(fn ->
        DepotS3.Minio.clean_bucket("secondary")
      end)

      {:ok, config_a: config, config_b: config_b}
    end

    test "copy", %{config_a: config_a, config_b: config_b} do
      filesystem_a = DepotS3.configure(config: config_a, bucket: "default")
      filesystem_b = DepotS3.configure(config: config_b, bucket: "secondary")

      :ok = Depot.write(filesystem_a, "test.txt", "Hello World")

      assert :ok =
               Depot.copy_between_filesystem(
                 {filesystem_a, "test.txt"},
                 {filesystem_b, "other.txt"}
               )

      assert {:ok, "Hello World"} = Depot.read(filesystem_b, "other.txt")
    end
  end
end
