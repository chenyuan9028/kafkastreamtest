package wordcount;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/7
 * \* Time: 下午2:04
 * \
 */
public class WindowConsumer {
    KafkaConsumer consumer;

    public WindowConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers",Constant.BROKERS);
        props.put("group.id","test_group-3");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","earliest");
//        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer(Serdes.String().deserializer());
        LongDeserializer longDeserializer = new LongDeserializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        consumer = new KafkaConsumer<>(props,windowedDeserializer, longDeserializer);

    }

    public Consumer getConsumer(){
        return consumer;
    }

    public String timestmp2date(Long timestamp){
        String str = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        str = sdf.format(date);
        return str;
    }

    public void consume(){

        consumer.subscribe(Arrays.asList(Constant.WINDOW_OUTPUT_TOPIC));
        while (true) {
            ConsumerRecords<Windowed<String>, Long> records = consumer.poll(100);
            for (ConsumerRecord<Windowed<String>, Long> record : records)
                System.out.printf("offset = %d, time=%s, key = %s,window_start=%d,window_end=%d, value = %d%n", record.offset(),timestmp2date(record.timestamp()), record.key().key(),
                        record.key().window().start(),record.key().window().end(),record.value());
        }
    }

    public static void main(String[] args){
        new WindowConsumer().consume();
    }
}
