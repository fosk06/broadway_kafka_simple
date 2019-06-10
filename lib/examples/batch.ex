defmodule KafkaBroadwaySimple.Example.Batch do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module: {KafkaBroadwaySimple.Producer, [
            offset: 90,
            topic: "topic1",
            partition: 0,
            worker_name: :first_producer
            ]},
          stages: 1
        ],
      ],
      processors: [
        default: [stages: 1]
      ],
      batchers: [
        cold_storage: [stages: 1, batch_size: 10],
      ]
    )
  end

  @impl true
  def handle_message(_, %Message{data: _data} = message, _) do
    # message.data.offset |> IO.inspect(label: "message offset ")
    # message.data.key |> IO.inspect(label: "message key ")
    message
    |> Message.put_batcher(:cold_storage)
  end

  @impl true
  def handle_batch(:cold_storage, messages, _batch_info, _context) do
    messages
  end

end
