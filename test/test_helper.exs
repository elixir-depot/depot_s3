{:ok, _} = DepotS3.Minio.start_link()
Process.sleep(1000)
DepotS3.Minio.initialize_bucket("default")
ExUnit.start()
