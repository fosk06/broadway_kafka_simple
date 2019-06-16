docker-compose exec kafka1  \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic topic1 --from-beginning --max-messages 102
