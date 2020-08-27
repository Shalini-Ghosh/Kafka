import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaStreamPractice {
    public static void main(String[] args){
        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trail-and-error");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> texts = builder.stream("input-stream-topic");
        texts.mapValues(s->s.toLowerCase()).to("output-stream-topic");
//        texts.mapValues(s->String.valueOf(s.toLowerCase()).to("output-stream-topic");
//
//        final KStream<byte[], String> stream1 = builder.stream("input-stream-topic");
//        final KStream<byte[], String> stream2 = stream1.mapValues(s-> s.toLowerCase());
////        stream2.to("output-stream-topic");
//        KStream<String, String> texts = builder.stream("input-stream-topic");
//        texts.mapValues(s->s.toLowerCase()).to("output-stream-topic");
//        builder.stream("input-stream-topic", Consumed.with(Serdes.String(), Serdes.String()))
//                .filter((key, value) -> value.contains("a"))
//                .mapValues(text -> text.toUpperCase())
//                .to("output-stream-topic", Produced.with(Serdes.String(), Serdes.String()));
        //builder.stream("input-stream-topic", Consumed.with(Serdes.String(), Serdes.String()))
//                .filter((key, value) -> value.contains("a"))
//                .mapValues(text -> text.toUpperCase())
//                .to("output-stream-topic", Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();



    }

}
