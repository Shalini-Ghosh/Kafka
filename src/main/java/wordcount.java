import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class wordcount {
    public static final String INTEGER_TOPIC_NAME = "test-input-topic";

    public static void main(String[] args){
        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "first-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        final StreamsBuilder builder= new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();


// Construct a `KStream` from the input topic "streams-plaintext-input", where message values
// represent lines of text (for the sake of this example, we ignore whatever may be stored
// in the message keys).
//        KStream<String, String> textLines = builder.stream("test-input-topic", Consumed.with(stringSerde, stringSerde));
//
//        KTable<String, Long> wordCounts = textLines
//                // Split each text line, by whitespace, into words.  The text lines are the message
//                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
//                // `flatMapValues` instead of the more generic `flatMap`.
//                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
//                // We use `groupBy` to ensure the words are available as message keys
//                .groupBy((key, value) -> value)
//                // Count the occurrences of each word (message key).
//                .count();
//
//// Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
//        wordCounts.toStream().to("test-output-topic", Produced.with(stringSerde, longSerde));
        final StreamsBuilder builder= new StreamsBuilder();
        KStream<Integer,String> streamLine= builder.stream(INTEGER_TOPIC_NAME,Consumed.with(Serdes.Integer(),Serdes.String()));
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
////        KTable<String, Long> wordCounts = textLines
////                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
//                .groupBy((key, word) -> word)
//                .count();
//        streamLine.flatMapValues(s-> Arrays.asList(s.toLowerCase().split("\\W+")))
//                .groupBy((key,word)->word)
//                .count()
//                .toStream().to("test-output-topic", Produced.with(Serdes.String(),Serdes.Long()));
        KTable<String, Long> wordCounts= streamLine.flatMapValues(s-> Arrays.asList(pattern.split(s.toLowerCase())))
//                .groupBy((key,word)->word,Serdes.String(),Serdes.Long())
                .groupBy((key,word)->word)
                .count();
        wordCounts.toStream().to("test-output-topic", Produced.with(Serdes.String(),Serdes.Long()));
//        streamLine.mapValues(value->String.valueOf(value.length())).to("test-output-topic");
        KafkaStreams streams= new KafkaStreams(builder.build(),props);
        streams.start();
    }
}
