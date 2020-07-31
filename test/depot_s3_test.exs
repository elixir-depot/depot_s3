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
end
