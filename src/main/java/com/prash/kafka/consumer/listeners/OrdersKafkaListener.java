package com.prash.kafka.consumer.listeners;


import com.prash.kafka.consumer.constants.KafkaConsumerConstants;
import com.prash.kafka.consumer.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class OrdersKafkaListener {

    private static final Logger LOG = LoggerFactory.getLogger(OrdersKafkaListener.class);

    /**
     *  This method consume 0 & 1 partitions of the given topic
     * @param order - Payload
     * @param partition - Header Value
     */
    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = KafkaConsumerConstants.ORDER_TOPIC,
                    partitions = {"0", "1"}
            ),
            groupId = KafkaConsumerConstants.GROUP_ID,
            containerFactory = "orderListenerFactory"
    )
    public void orderTopicListenerWithMultiplePartitions(@Payload Order order,
                                                         @Header(KafkaHeaders.PARTITION) int partition) {
        LOG.info("Received Order [{}] from partition [{}]", order, partition);
        //Additional logic to process the message can be written here.
    }

    /**
     *  This method consume 2nd partition of the given topic
     * @param order - Payload
     * @param partition - Partition value
     * @param offset - Offset value
     */
    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = KafkaConsumerConstants.ORDER_TOPIC,
                    partitions = {"2"}
            ),
            groupId = KafkaConsumerConstants.GROUP_ID,
            containerFactory = "orderListenerFactory"
    )
    public void orderTopicListener(@Payload Order order,
                                   @Header(KafkaHeaders.PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) int offset) {
        LOG.info("Received Order [{}] from the partition [{}] with offset[{}]", order, partition, offset);
        //Additional logic to process the message can be written here.
    }
}
