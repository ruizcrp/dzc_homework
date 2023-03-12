package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.streams.StreamsConfig;
import org.example.data.FHVRide;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class JsonProducerFHV {
    private Properties props = new Properties();
    public JsonProducerFHV() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-75m1o.europe-west3.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    public List<FHVRide> getRides() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/fhv_tripdata_2019-01.csv");
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1);
        return reader.readAll().stream().map(arr -> new FHVRide(arr))
                .collect(Collectors.toList());

    }

    public void publishRides(List<FHVRide> rides) throws ExecutionException, InterruptedException {
        KafkaProducer<String, FHVRide> kafkaProducer = new KafkaProducer<String, FHVRide>(props);
        for(FHVRide ride: rides) {
            ride.pickup_datetime = LocalDateTime.now().minusMinutes(20);
            ride.dropOff_datetime = LocalDateTime.now();
            var record = kafkaProducer.send(new ProducerRecord<>("fhvrides", String.valueOf(ride.DOlocationID), ride), (metadata, exception) -> {
                if(exception != null) {
                    System.out.println(exception.getMessage());
                }
            });
            System.out.println(record.get().offset());
            System.out.println(ride.DOlocationID);
            Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new JsonProducerFHV();
        var rides = producer.getRides();
        producer.publishRides(rides);
    }
}