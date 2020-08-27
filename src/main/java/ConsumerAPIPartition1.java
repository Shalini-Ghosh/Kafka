import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Arrays;
import java.util.Properties;
public class ConsumerAPIPartition1 {
            public static void main(String[] args){
            Properties props= new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG,"Trial&Error_other");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            TopicPartition partition1 = new TopicPartition("input-file-handling",1);
            KafkaConsumer<Integer,String> sampleConsumer = new KafkaConsumer<>(props);
            sampleConsumer.assign(Arrays.asList(partition1));
            while (true){
                ConsumerRecords<Integer,String> records = sampleConsumer.poll(100);
                for(ConsumerRecord<Integer,String> record : records){
                    System.out.println(record.toString());

            }
        }
    }
}
