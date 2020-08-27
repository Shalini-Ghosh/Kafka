import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
public class ProducerAPI {
    public static void main(String[] args){
        Properties props= new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Integer,String> sampleProducer = new KafkaProducer<Integer, String>(props);
        for(int i=-10;i<=10;i++){
            if(i<0){
                sampleProducer.send(new ProducerRecord<Integer, String>("input-partitioned-topic-1",0,new Integer(i),"Value(" +i+ ")is negative"));
            }
            else if(i==0){
                sampleProducer.send(new ProducerRecord<Integer, String>("input-partitioned-topic-1",1,new Integer(i),"Value(" +i+ ")is zero"));
            }
            else{
                sampleProducer.send(new ProducerRecord<Integer, String>("input-partitioned-topic-1",2,new Integer(i),"Value(" +i+ ")is positive"));
            }
        }
        sampleProducer.close();
    }
}




