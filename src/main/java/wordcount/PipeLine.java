package wordcount;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/7
 * \* Time: 下午2:32
 * \
 */
public class PipeLine {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cluster-stream-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BROKERS);
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
        props.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());

        final KafkaStreams streams = createStreams(props);
//        streams.cleanUp();
        streams.start();


        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
//                wordCountAppService.stop();
            } catch (Exception e) {
                // ignored
            }
        }));

    }

    static KafkaStreams createStreams(final Properties streamsConfiguration) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        //kafka-client版本：0.10.2.1
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream(stringSerde, stringSerde, Constant.SOURCE_TOPIC);
        KGroupedStream<String, String> kGroupedStream = source.flatMapValues((value) -> {
            String[] arr = value.split("\\|");
            int length = arr.length;
            return Arrays.asList(arr[length - 1]);
        }).groupBy((key, value) -> value, stringSerde, stringSerde);

        KTable<String, Long> ip_count = kGroupedStream.count("ip_count");
        ip_count.toStream().to(stringSerde, longSerde, Constant.OUTPUT_TOPIC);
        return new KafkaStreams(builder, streamsConfiguration);
    }
}
