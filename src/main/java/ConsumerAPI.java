import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;



public class ConsumerAPI {
    private static Object Outputs;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        TopicPartition partition0= new TopicPartition("input-partitioned-topic",0);
        TopicPartition partition1= new TopicPartition("input-partitioned-topic",1);
        TopicPartition partition2= new TopicPartition("input-partitioned-topic",2);
        KafkaConsumer<Integer,String> sampleConsumer = new KafkaConsumer<Integer, String>(props);
        sampleConsumer.assign(Arrays.asList(partition0,partition1,partition2));
        //sampleConsumer.subscribe(Arrays.asList("input-partitioned-topic"));
        while(true){
            ConsumerRecords<Integer, String> Output=sampleConsumer.poll(10);
            for(ConsumerRecord<Integer, String> Outputs : Output)
                System.out.println(Output.toString());
        }

    }
}
