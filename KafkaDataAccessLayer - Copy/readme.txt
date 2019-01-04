 #./bin/kafka-topics --create --topic  getlink --zookeeper localhost:2181 --partitions 1 --replication-factor 1

 ./bin/kafka-topics --create --topic  profilelinkingervice --zookeeper localhost:2181 --partitions 1 --replication-factor 1

 ./bin/kafka-topics --create --topic  profilelinkingervice-return --zookeeper localhost:2181 --partitions 1 --replication-factor 1


#./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic getlink

 ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic profilelinkingservice-return --from-beginning