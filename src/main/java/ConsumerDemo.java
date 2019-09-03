import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");     // kafka server host 및 port
        properties.put("session.timeout.ms", "10000");             // session 설정
        properties.put("group.id", "test_topic");                // topic 설정
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");    // key deserializer
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  // value deserializer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test_topic"));
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(500);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String s = consumerRecord.topic();
                if(s.equals("test_topic")) {
                    System.out.println(consumerRecord.value());
                } else {
                    throw new IllegalStateException("get message on topic" + s);
                }
            }
        }

    }
}
