package com.example.demosyncmessage;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

public class MyConsumer extends KafkaConsumer<byte[], byte[]> {
    public MyConsumer(Properties properties) {
        super(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    public  ConsumerRecords<byte[], byte[]> pollEnhanced(Duration dur) {
        ConsumerRecords<byte[], byte[]> polled = poll(dur);

        return polled;
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        super.assign(partitions);
    }

    @Override
    public void subscribe(Pattern pattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(Duration timeout) {
        super.close(timeout);
    }
}
