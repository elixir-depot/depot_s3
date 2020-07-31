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

  defmodule StreamUpload do
    @enforce_keys [:config, :path]
    defstruct config: nil, path: nil, opts: []

    defimpl Collectable do
      defp upload_part(config, path, id, index, data, opts) do
        %{headers: headers} =
          ExAws.S3.upload_part(config.bucket, path, id, index, data, opts)
          |> ExAws.request!(config.config)

        {_, etag} = Enum.find(headers, fn {k, _v} -> String.downcase(k) == "etag" end)
        etag
      end

      def into(%{config: config, path: path, opts: opts} = stream) do
        {:ok, %{body: %{upload_id: upload_id}}} =
          ExAws.S3.initiate_multipart_upload(config.bucket, path, opts)
          |> ExAws.request(config.config)

        collector_fun = fn
          %{acc: acc} = data, {:cont, elem}
          when is_binary(elem) and byte_size(acc) + byte_size(elem) >= 5 * 1024 * 1024 ->
            etag = upload_part(config, path, data.upload_id, data.index, acc <> elem, opts)
            %{data | acc: "", index: data.index + 1, etags: [{data.index, etag} | data.etags]}

          %{acc: acc} = data, {:cont, elem} ->
            %{data | acc: acc <> elem}

          %{acc: acc} = data, :done ->
            data =
              if byte_size(acc) == 0 do
                data
              else
                etag = upload_part(config, path, data.upload_id, data.index, acc, opts)
                %{data | acc: "", index: data.index + 1, etags: [{data.index, etag} | data.etags]}
              end

            {:ok, _} =
              ExAws.S3.complete_multipart_upload(
                config.bucket,
                path,
                data.upload_id,
                Enum.sort_by(data.etags, &elem(&1, 0))
              )
              |> ExAws.request(config.config)

            stream

          _set, :halt ->
            :ok
        end

        {%{upload_id: upload_id, acc: "", index: 0, etags: []}, collector_fun}
      end
    end
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
  def write_stream(%Config{} = config, path, opts) do
    {:ok,
     %StreamUpload{
       config: config,
       path: path,
       opts: opts
     }}
  end

  @impl Depot.Adapter
  def read(%Config{} = config, path) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    operation = ExAws.S3.get_object(config.bucket, path)

    case ExAws.request(operation, config.config) do
      {:ok, %{body: body}} -> {:ok, body}
      {:error, {:http_error, 404, _}} -> {:error, :enoent}
      rest -> rest
    end
  end

  # TODO: This next section has been copied from ex_aws_s3 and can likely be replaced
  # as soon as there is a release on this merge request:
  # https://github.com/ex-aws/ex_aws_s3/pull/60/commits/6b9fdac73b62dee14bffb939965742f2576f2a7b#diff-50dc7f8117b1be05295369ca23e8fa73
  @impl Depot.Adapter
  def read_stream(%Config{} = config, path, opts) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    with {:ok, :exists} <- file_exists(config, path) do
      op = ExAws.S3.download_file(config.bucket, path, "", opts)

      stream =
        op
        |> ExAws.S3.Download.build_chunk_stream(config.config)
        |> Task.async_stream(
          fn boundaries ->
            ExAws.S3.Download.get_chunk(op, boundaries, config.config)
          end,
          max_concurrency: Keyword.get(op.opts, :max_concurrency, 8),
          timeout: Keyword.get(op.opts, :timeout, 60_000)
        )
        |> Stream.map(fn
          # Download.get_chunk/3 uses ExAws.request! so if we get here it is
          # successful otherwise it has already risen an error
          {:ok, {_start_byte, chunk}} ->
            chunk
        end)

      {:ok, stream}
    else
      {:ok, :missing} -> {:error, :enoent}
    end
  end

  @impl Depot.Adapter
  def delete(%Config{} = config, path) do
    path = Depot.RelativePath.join_prefix(config.prefix, path)

    operation = ExAws.S3.delete_object(config.bucket, path)

    case ExAws.request(operation, config.config) do
      {:ok, _} -> :ok
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
      {:error, {:http_error, 404, _}} -> {:error, :enoent}
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
