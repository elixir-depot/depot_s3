defmodule DepotS3 do
  @moduledoc """
  Depot Adapter for the local filesystem.

  ## Direct usage

      config = [
        access_key_id: "key",
        secret_access_key: "secret",
        scheme: "https://",
        region: "eu-west-1",
        host: "s3.eu-west-1.amazonaws.com",
        port: 443
      ]
      filesystem = DepotS3.configure(config: config, bucket: "default")
      :ok = Depot.write(filesystem, "test.txt", "Hello World")
      {:ok, "Hello World"} = Depot.read(filesystem, "test.txt")

  ## Usage with a module

      defmodule S3FileSystem do
        use Depot,
          adapter: DepotS3,
          bucket: "default",
          config: [
            access_key_id: "key",
            secret_access_key: "secret",
            scheme: "https://",
            region: "eu-west-1",
            host: "s3.eu-west-1.amazonaws.com",
            port: 443
          ]
      end

      S3FileSystem.write("test.txt", "Hello World")
      {:ok, "Hello World"} = S3FileSystem.read("test.txt")
  """

  defmodule Config do
    @moduledoc false
    defstruct config: nil, bucket: nil, prefix: nil
  end

  @behaviour Depot.Adapter

  @impl Depot.Adapter
  def starts_processes, do: false

  @impl Depot.Adapter
  def configure(opts) do
    config = %Config{
      config: Keyword.fetch!(opts, :config),
      bucket: Keyword.fetch!(opts, :bucket),
      prefix: Keyword.get(opts, :prefix, "/")
    }

    {__MODULE__, config}
  end

  @impl Depot.Adapter
  def write(%Config{} = config, path, contents) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    operation = ExAws.S3.put_object(config.bucket, path, contents)

    with {:ok, _} <- ExAws.request(operation, config.config) do
      :ok
    end
  end

  @impl Depot.Adapter
  def read(%Config{} = config, path) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    operation = ExAws.S3.get_object(config.bucket, path)

    with {:ok, %{body: body}} <- ExAws.request(operation, config.config) do
      {:ok, body}
    end
  end

  @impl Depot.Adapter
  def delete(%Config{} = config, path) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    operation = ExAws.S3.delete_object(config.bucket, path)

    case ExAws.request(operation, config.config) do
      {:ok, _} -> :ok
      {:error, %{status_code: 404}} -> :ok
      rest -> rest
    end
  end

  @impl Depot.Adapter
  def move(%Config{} = config, source, destination) do
    with :ok <- copy(config, source, destination) do
      delete(config, source)
    end
  end

  @impl Depot.Adapter
  def copy(%Config{} = config, source, destination) do
    source = Depot.RelativePath.join_prefix(config.prefix, source)
    destination = Depot.RelativePath.join_prefix(config.prefix, destination)

    operation = ExAws.S3.put_object_copy(config.bucket, destination, config.bucket, source)

    case ExAws.request(operation, config.config) do
      {:ok, _} -> :ok
      {:error, %{status_code: 404}} -> :ok
      rest -> rest
    end
  end

  @impl Depot.Adapter
  def file_exists(%Config{} = config, path) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    operation = ExAws.S3.head_object(config.bucket, path)

    case ExAws.request(operation, config.config) do
      {:ok, _} -> {:ok, :exists}
      {:error, {:http_error, 404, _}} -> {:ok, :missing}
      rest -> rest
    end
  end

  @impl Depot.Adapter
  def list_contents(%Config{} = config, path) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    operation = ExAws.S3.list_objects(config.bucket, prefix: path)

    with {:ok, %{body: %{contents: files}}} <- ExAws.request(operation, config.config) do
      contents =
        for file <- files do
          {:ok, dt, 0} = DateTime.from_iso8601(file.last_modified)

          %Depot.Stat.File{
            name: Depot.RelativePath.strip_prefix(config.prefix, file.key),
            size: String.to_integer(file.size),
            mtime: dt
          }
        end

      {:ok, contents}
    else
      rest -> rest
    end
  end
end
