import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

public class WhiteSpaceCount {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "white-space");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
       KStream<String, String> texts = builder.stream("input-stream-topic",Consumed.with(Serdes.String(), Serdes.String()));
        //texts.foreach((key, value)-> value.equalsIgnoreCase("\\W+"));
//        texts.filter((k,v)-> {
//             Arrays.asList(v.equalsIgnoreCase(" "));
//            return true;
//        });

//        builder.stream("input-whitespace-stream", Consumed.with(Serdes.String(), Serdes.String()))
//                .foreach((key, value)-> value.equalsIgnoreCase("\\W+"));

//        KStream<String,Long> whitespace= (KStream<String, Long>) builder.stream("input-whitespace-stream", Consumed.with(Serdes.String(), Serdes.String()))
//                .filter((key, value) -> value.equalsIgnoreCase("kafka"))
//                .groupBy((k,v)->v)
//                .count();
        //whitespace.to();
//        builder.stream("input-stream-topic", Consumed.with(Serdes.String(), Serdes.String()))
//              .filter((key, value) -> value.contains("a"))
//    .groupBy((k,v)->v)
//                .count();
//                .toStream()
//                .to("output-whitespace-topic",Consumed.with(Serdes.String(), Serdes.Long()));
//                .mapValues(v->v.toString())
//                .toStream()
//                .to("output-whitespace-topic");
//        texts.flatMapValues(value ->
//                Arrays.asList(value.equalsIgnoreCase("\\W+")))
//                .groupBy((k,v)->v)
//                .count()
//                .toStream()
//                .to("input-stream-topic");
//        KStream<String, String> whitespace = texts.foreach(
//                (key,s)->s.equalsIgnoreCase(" "));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();



    }
}

