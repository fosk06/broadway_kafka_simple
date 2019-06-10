defmodule KafkaBroadwaySimple.Producer do
  use GenStage
  alias Broadway.{Message, Acknowledger, Producer}

  @behaviour Acknowledger
  @behaviour Producer

  def start_link(args) do
    GenStage.start_link(__MODULE__, args)
  end

  @impl true
  def init(state) do
    worker_name = Keyword.get(state, :worker_name, :producer)
    KafkaEx.create_worker(worker_name)
    {:producer, state}
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    {last_offset,messages} = fetch(demand,state)
    state = Keyword.put(state, :offset, last_offset)
    worker_name = Keyword.get(state, :worker_name, :producer)
    messages = messages |> produce_brodway_messages(worker_name)
    {:noreply, messages, state}
  end

  def handle_demand(_demand, offset) do
    "handle demand" |> IO.inspect(label: "handle")
    {:noreply, [], offset}
  end

  defp produce_brodway_messages(messages,worker_name) do
    messages
    |> Enum.map(fn kafka_message ->
      ack = %{consumer_group: "group-id", topic: kafka_message.topic, partition: kafka_message.partition, offset: kafka_message.offset, metadata: ""}
      %Message{
        data: %{key: kafka_message.key, value: kafka_message.value, offset: kafka_message.offset},
        metadata: Map.take(kafka_message, [:attributes,:crc,:offset, :topic]),
        acknowledger: {__MODULE__,[worker_name: worker_name], ack}
      }
    end)
  end

  def fetch(demand,[offset: offset,topic: topic,partition: partition, worker_name: worker_name]) do
    messages = KafkaEx.stream(topic,partition, offset: offset, auto_commit: false, worker_name: worker_name)
    |> Enum.take(demand)
    |> Enum.map(fn msg -> Map.put(msg, :topic, topic) end)
    |> Enum.map(fn msg -> Map.put(msg, :partition, partition) end)
    last_offset = List.last(messages) |> Map.fetch!(:offset)
    {last_offset+1,messages}
  end

  @impl Acknowledger
  def ack(_ack_ref, successful, _failed) do
    for %{acknowledger: {_,[worker_name: worker_name], ack}} <- successful do
      ack_successful_message(worker_name, ack)
    end
    :ok
  end

  defp ack_successful_message(worker_name, ack) do
    ack.offset |> IO.inspect(label: "ack message with offset")
    KafkaEx.offset_commit(worker_name, ack)
  end


  @impl Producer
  def prepare_for_draining(_state) do
    :ok
  end

end



