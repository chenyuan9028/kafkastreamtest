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
import java.util.Properties;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/7
 * \* Time: 下午2:01
 * \
 */

/**
 * 将中间topic为转换为流，进行第二级粒度的窗口计算，生成新的窗口流，比如每分钟生成一个窗口，并输出到一个新的topic
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cluster-stream-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BROKERS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,10*1024*1024L);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,5000);

        final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
        props.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());

        final KafkaStreams streams = createStreams(props,Constant.SECOND_INTERVAL);
        streams.start();


        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception e) {
                // ignored
            }
        }));

    }

    static KafkaStreams createStreams(final Properties streamsConfiguration,long windowsize) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);

        //kafka-client版本：0.10.2.1
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,Long> source = builder.stream(stringSerde,longSerde,Constant.MID_TOPIC);
        KGroupedStream<String,Long> kGroupedStream = source.groupBy((key,value)->key,stringSerde,longSerde);

        KTable<Windowed<String>, Long> ip_windowed_count = kGroupedStream.aggregate(
                ()->0L,(k,v,n)->v+n, TimeWindows.of(windowsize),longSerde,
                "windowed-ip-count-store");
        ip_windowed_count.toStream().to(windowedSerde,longSerde,Constant.WINDOW_OUTPUT_TOPIC);
        return new KafkaStreams(builder,streamsConfiguration);




    }
}
