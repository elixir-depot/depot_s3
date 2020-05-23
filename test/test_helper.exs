{:ok, _} = DepotS3.Minio.start_link()
DepotS3.Minio.initialize_bucket("default")
ExUnit.start()
