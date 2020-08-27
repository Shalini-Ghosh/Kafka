import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;
public class ProducerAPIFile {
    public static String TOPIC_NAME = "input-file-handling";
    public static void main(String[] args) throws Exception{
        Properties props= new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Integer,String> sampleProducer = new KafkaProducer<Integer, String>(props);
        InputStream input = null;
        try{
            input = new FileInputStream("C:\\kafka\\config\\InputSupplier.txt");
            Scanner myReader = new Scanner(input);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                if(new Integer(data)<0){
                    sampleProducer.send(new ProducerRecord<Integer, String>(TOPIC_NAME,0,new Integer(data),"Value(" +data+ ")is negative"));
                }
                else if(new Integer(data)==0){
                    sampleProducer.send(new ProducerRecord<Integer, String>(TOPIC_NAME,1,new Integer(data),"Value(" +data+ ")is zero"));
                }
                else{
                    sampleProducer.send(new ProducerRecord<Integer, String>(TOPIC_NAME,2,new Integer(data),"Value(" +data+ ")is positive"));
                }
            }
        }catch (Exception e){
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        sampleProducer.close();
    }
}



