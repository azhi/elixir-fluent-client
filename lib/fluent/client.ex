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

  def handle_cast({:send, tag, data}, state) do
    state = ensure_connected!(state)
    state = send(tag, data, state)
    {:noreply, state}
  end

  def handle_call({:send, tag, data}, _from, state) do
    state = ensure_connected!(state)
    state = send(tag, data, state)
    {:reply, :ok, state}
  end

  defp ensure_connected!(%State{socket: nil, options: options} = state) do
    case connect(options) do
      {:ok, socket} ->
        %State{state | socket: socket}
      {:error, error} ->
        raise RuntimeError, "Couldn't connect to fluent. Error: #{error}"
    end
  end

  defp ensure_connected!(%State{socket: socket} = state) do
    # Nothing to do
    state
  end

  defp connect(options, retry \\ 1) do
    case Socket.TCP.connect(options[:host] || "localhost", options[:port] || 24224, packet: 0) do
      {:ok, socket} -> {:ok, socket}
      {:error, error} ->
        if retry > 5 do
          {:error, error}
        else
          :timer.sleep round(:math.pow(2, retry - 1) * 1000)
          connect(options, retry + 1)
        end
    end
  end

  defp send(tag, data,  %State{socket: socket, options: options, serializer: serializer} = state) do
    packet = serializer.([tag, now(), data])
    case Socket.Stream.send(socket, packet) do
      :ok -> state
      {:error, :closed} ->
        Socket.Stream.close(socket)
        new_state = ensure_connected!(%State{state | socket: nil})
        send(tag, data, new_state)
      {:error, error} ->
        raise RuntimeError, "Error when sending data to fluent: #{error}"
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
