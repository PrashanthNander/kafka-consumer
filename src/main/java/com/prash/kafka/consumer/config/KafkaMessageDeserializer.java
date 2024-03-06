package com.prash.kafka.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prash.kafka.consumer.model.Order;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaMessageDeserializer extends ErrorHandlingDeserializer<Order> implements Deserializer<Order> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Order deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(new String(bytes,
                    StandardCharsets.UTF_8), Order.class);
        } catch (Exception e) {
            throw new SerializationException("Error while deserializing byte[] to Order" + e);
        }
    }

    @Override
    public void close() {
    }

}
