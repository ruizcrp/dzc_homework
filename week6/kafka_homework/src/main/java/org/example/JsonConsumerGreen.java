package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.data.GreenRide;
import org.example.data.MyStreamObject;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Properties;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
public class JsonConsumerGreen {

    private Properties props = new Properties();
    private KafkaConsumer<String, MyStreamObject> consumer;
    public JsonConsumerGreen() {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-75m1o.europe-west3.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_tutorial_example.jsonconsumer.v2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE,MyStreamObject.class);
        consumer = new KafkaConsumer<String, MyStreamObject>(props);
        consumer.subscribe(List.of("greenrides-pulocation-count"));
      
    }

    public void consumeFromKafka() {
        System.out.println("Consuming form kafka started");
        var results = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
        var i = 0;
        do {
            System.out.println("OUTSIDE");
            System.out.println(results);

            for(ConsumerRecord<String, MyStreamObject> result: results) {
                System.out.println("INSIDE");
                System.out.println(result.value());
            }
            results =  consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            System.out.println("RESULTS:::" + results.count());
            i++;
        }
        while(!results.isEmpty() || i < 10);
    }

    public static void main(String[] args) {
        JsonConsumerGreen jsonConsumer = new JsonConsumerGreen();
        jsonConsumer.consumeFromKafka();
    }
}
