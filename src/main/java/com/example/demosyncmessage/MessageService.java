package com.example.demosyncmessage;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class MessageService {
    @Value("${spring.kafka.properties.sasl.jaas.config}")
    String sjc;

    public List<MessageDto> syncMessages() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.168.10.23:30861,10.168.10.21:31161,10.168.10.15:30933");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"tDwoMrQpl4CMyoK\";");
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafdrop-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            String topic = "test-3";
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
                long currentOffset = 0;
                long latestOffset = latestOffsets.get(partition);
                while (currentOffset < latestOffset) {
                    var polled = consumer.poll(Duration.ofMillis(100)).records(partition);
                    if (!polled.isEmpty()) {
                        rawRecords.addAll(polled);
                        log.info("get polled {}", polled);
                        currentOffset = polled.get(polled.size() - 1).offset() + 1;
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
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("err");
        }
    }

    public Flux<MessageDto> syncMessagesFlux() {
        return Flux.create(fluxSink -> {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.168.10.23:30861,10.168.10.21:31161,10.168.10.15:30933");
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"tDwoMrQpl4CMyoK\";");
            properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafdrop-consumer-" + UUID.randomUUID());
            properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
                String topic = "test-3";
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                var rawRecords = new ArrayList<ConsumerRecord<byte[], byte[]>>();
                List<TopicPartition> partitions = partitionInfos
                        .stream()
                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                        .toList();
                var latestOffsets = consumer.endOffsets(partitions);
                consumer.assign(partitions);
                for (var partition : partitions) {
                    consumer.seek(partition, 0);
                    long currentOffset = 0;
                    long latestOffset = latestOffsets.get(partition);
                    log.info("thread: {}", Thread.currentThread().getName());
                    while (currentOffset < latestOffset) {
                        var polled = consumer.poll(Duration.ofMillis(100)).records(partition);
                        if (!polled.isEmpty()) {
                            currentOffset = polled.get(polled.size() - 1).offset() + 1;
                            for (var rec : polled) {
                                fluxSink.next(MessageDto.builder()
                                        .partition(rec.partition())
                                        .offset(rec.offset())
                                        .key(new String(rec.key(), StandardCharsets.UTF_8))
                                        .value(new String(rec.value(), StandardCharsets.UTF_8))
                                        .timestamp(rec.timestamp())
                                        .build());
                            }
                        }
                    }
                }
                fluxSink.complete();
            } catch (Exception ex) {
//                ex.printStackTrace();
                fluxSink.error(ex);
            }
        });
//        return Flux.interval(Duration.ofSeconds(1))
//                .zipWith(Flux.range(0, 50))
//                .map(tuple -> MessageDto.builder()
//                        .partition(1)
//                        .offset(1)
//                        .key("key")
//                        .value("value")
//                        .timestamp(100000)
//                        .build());
    }

    public SseEmitter syncMessagesSseEmitter() {
        SseEmitter emitter = new SseEmitter(0L);
//        ResponseBodyEmitter emitter = new ResponseBodyEmitter(0L);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.168.10.23:30861,10.168.10.21:31161,10.168.10.15:30933");
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"tDwoMrQpl4CMyoK\";");
            properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafdrop-consumer-" + UUID.randomUUID());
            properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
                String topic = "test-3";
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                var rawRecords = new ArrayList<ConsumerRecord<byte[], byte[]>>();
                List<TopicPartition> partitions = partitionInfos
                        .stream()
                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                        .toList();
                var latestOffsets = consumer.endOffsets(partitions);
                consumer.assign(partitions);
                for (var partition : partitions) {
                    consumer.seek(partition, 0);
                    long currentOffset = 0;
                    long latestOffset = latestOffsets.get(partition);
                    while (currentOffset < latestOffset) {
                        var polled = consumer.poll(Duration.ofMillis(100)).records(partition);
                        if (!polled.isEmpty()) {
                            currentOffset = polled.get(polled.size() - 1).offset() + 1;
                            for (var rec : polled) {
                                var msgDto = MessageDto.builder()
                                        .partition(rec.partition())
                                        .offset(rec.offset())
                                        .key(new String(rec.key(), StandardCharsets.UTF_8))
                                        .value(new String(rec.value(), StandardCharsets.UTF_8))
                                        .data("abc")
                                        .timestamp(rec.timestamp())
                                        .build();
                                emitter.send(
                                        SseEmitter.event()
                                                .id(String.valueOf(new Date().getTime()))
                                                .name("message")
                                                .data(msgDto, MediaType.APPLICATION_JSON)
                                                .build()
                                );
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.info("Emitter shut down by Exception: {}", e.getMessage());
                emitter.completeWithError(e);
            } finally {
                emitter.complete();
                executor.shutdown();
            }

        });
        return emitter;
    }
}

//try {
//                for (int i = 0; true; i++){
//                    emitter.send(MessageDto.builder()
//                            .partition(1)
//                            .offset(1)
//                            .key("key")
//                            .value("value")
//                            .timestamp(1000000)
//                            .build());
//                    log.info("emitter: {}", i);
//                    Thread.sleep(1000);
//                }
//            }catch (Exception e) {
//                log.error(e.getMessage());
//                emitter.completeWithError(e);
//            }finally {
//                emitter.complete();
//                executor.shutdown();
//            }
