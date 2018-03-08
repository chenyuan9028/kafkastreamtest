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
 * \* Time: 下午2:01
 * \
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cluster-stream-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BROKERS);
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,10*1024*1024L);
//        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,60*1000);

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
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);



        //kafka-client版本：0.10.2.1

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,String> source = builder.stream(stringSerde,stringSerde,Constant.SOURCE_TOPIC);
        KGroupedStream<String,String> kGroupedStream = source.flatMapValues((value) ->{
            String[] arr = value.split("\\|");
            int length = arr.length;
            System.out.println(arr[length -1]);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return Arrays.asList(arr[length -1]);
        }).groupBy((key,value)->value,stringSerde,stringSerde);

        KTable<Windowed<String>, Long> ip_windowed_count = kGroupedStream.count(TimeWindows.of(60000L).advanceBy(10000L),
                "windowed-ip-count-store");
        ip_windowed_count.toStream().to(windowedSerde,longSerde,Constant.WINDOW_OUTPUT_TOPIC);
        return new KafkaStreams(builder,streamsConfiguration);



        ////kafka-client版本：1.0.0
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String,String> source = builder.stream(SOURCE_TOPIC);
//        KGroupedStream<String,String> kGroupedStream = source.flatMapValues((value) ->{
//            String[] arr = value.split("\\|");
//            int length = arr.length;
////            System.out.println(arr[length -1]);
////            try {
////                Thread.sleep(1000L);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//            return Arrays.asList(arr[length -1]);
//        }).groupBy((key,value)->value,Serialized.with(stringSerde,stringSerde));
//        KTable<Windowed<String>, Long> ip_windowed_count = kGroupedStream
//                .windowedBy(TimeWindows.of(60000L))
//                .count(Materialized.as("ip_windowed_count4"));
//
//        ip_windowed_count.toStream().to("cluster_ip_count3",Produced.with(windowedSerde,Serdes.Long()));
//        return new KafkaStreams(builder.build(),streamsConfiguration);
    }
}
