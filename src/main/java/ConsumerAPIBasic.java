import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Arrays;
import java.util.Properties;
public class ConsumerAPIBasic {

          public static void main(String[] args){
            Properties props= new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG,"FirstTrial&Error");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            KafkaConsumer<Integer,String> sampleConsumer = new KafkaConsumer<>(props);
            sampleConsumer.subscribe(Arrays.asList("input-partitioned-topic-1"));
            while (true){
                ConsumerRecords<Integer,String> records = sampleConsumer.poll(100);
                for(ConsumerRecord<Integer,String> record : records){
                    System.out.println(record.toString());
                }
            }
        }
}

