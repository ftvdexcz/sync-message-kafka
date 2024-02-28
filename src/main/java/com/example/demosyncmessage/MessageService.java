package com.example.demosyncmessage;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MessageService {
    @Value("${spring.kafka.properties.sasl.jaas.config}")
    String sjc;

    public List<MessageDto> syncMessages(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.162:29093");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, sjc);
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafdrop-consumer-" + UUID.randomUUID().toString());
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try(KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)){
            String topic = "test-1";
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            var rawRecords = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            List<TopicPartition> partitions = partitionInfos
                    .stream()
                    .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                    .toList();
            var latestOffsets = consumer.endOffsets(partitions);
            consumer.assign(partitions);
            partitions.forEach(partition -> {
                consumer.seek(partition, 0);
                long currentOffset = -1;
                long latestOffset = Math.max(0, latestOffsets.get(partition) - 1);
                while (currentOffset < latestOffset) {
                    var polled = consumer.poll(Duration.ofMillis(100)).records(partition);
                    if (!polled.isEmpty()) {
                        rawRecords.addAll(polled);
                        currentOffset = polled.get(polled.size() - 1).offset();
                    }
                }
            });
            return rawRecords
                    .stream()
                    .map(rec -> MessageDto.builder()
                            .partition(rec.partition())
                            .offset(rec.offset())
                            .key(new String(rec.key(), StandardCharsets.UTF_8))
                            .value(new String(rec.value(), StandardCharsets.UTF_8))
                            .timestamp(rec.timestamp())
                            .build())
                    .toList();
        }catch (Exception ex){
            ex.printStackTrace();
            throw new RuntimeException("err");
        }
    }
}
