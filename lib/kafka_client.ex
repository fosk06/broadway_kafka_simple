defmodule KafkaBroadwaySimple.KafkaClient do
  def get_earliest_offset(state) do
    KafkaEx.earliest_offset(state[:topic], state[:partition], state[:worker_name])
    |> extract_offset()
  end

  def get_latest_offset(state) do
    KafkaEx.latest_offset(state[:topic], state[:partition], state[:worker_name])
    |> extract_offset()
  end

  defp extract_offset(offset_payload) do
    offset_payload
    |> List.first()
    |> Map.fetch!(:partition_offsets)
    |> List.first()
    |> Map.fetch!(:offset)
    |> List.first()
  end

  def fetch(demand,[offset: offset,topic: topic,partition: partition, worker_name: worker_name]) do
    messages = KafkaEx.stream(topic,partition, offset: offset, auto_commit: false, worker_name: worker_name)
    |> Enum.take(demand)
    |> Enum.map(fn msg -> Map.put(msg, :topic, topic) end)
    |> Enum.map(fn msg -> Map.put(msg, :partition, partition) end)
    last_offset = List.last(messages) |> Map.fetch!(:offset)
    {last_offset+1,messages}
  end
end
