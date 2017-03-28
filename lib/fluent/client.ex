defmodule Fluent.Client do
  use GenServer

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  def start_link(name, options) do
    GenServer.start_link(__MODULE__, options, name: name)
  end

  defmodule State do
    defstruct socket: nil, options: [], serializer: :msgpack
  end

  def init(options) do
    serializer = serializer(options[:serializer] || :msgpack)
    {:ok, %State{options: options, serializer: serializer}}
  end

  def handle_cast(msg, %State{socket: nil, options: options} = state) do
    socket = connect(options)
    handle_cast(msg, %State{ state | socket: socket})
  end

  def handle_cast({:send, tag, data}, state) do
    case send(tag, data, state) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_call(call, from, %State{socket: nil, options: options} = state) do
    socket = connect(options)
    handle_call(call, from, %State{ state | socket: socket})
  end

  def handle_call({:send, tag, data}, _from, state) do
    case send(tag, data, state) do
      {:ok, state} ->
        {:reply, :ok, state}
      {:error, reason} ->
        {:stop, reason, :error, state}
    end
  end

  defp connect(options) do
    Socket.TCP.connect!(options[:host] || "localhost",options[:port] || 24224, packet: 0)
  end

  defp send(tag, data,  %State{socket: socket, options: options, serializer: serializer} = state, retry \\ 1) do
    packet = serializer.([tag, now, data])
    case Socket.Stream.send(socket, packet) do
      :ok -> {:ok, state}
      {:error, :closed} ->
        if retry > 5 do
          {:error, {:socket_error, :connect_retry_exhausted}}
        else
          new_state = %State{state | socket: connect(options)}
          send(tag, data, new_state, retry + 1)
        end
      {:error, reason} ->
        {:error, {:socket_error, reason}}
    end
  end

  defp serializer(:msgpack), do: &Msgpax.pack!/1
  defp serializer(:json), do: &Poison.encode!/1
  defp serializer(f) when is_function(f, 1), do: f

  defp now do
    {msec, sec, _ } = :os.timestamp
    msec * 1000000 + sec
  end
end
