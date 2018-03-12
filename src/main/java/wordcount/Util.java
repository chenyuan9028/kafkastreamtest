package wordcount;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/9
 * \* Time: 上午10:20
 * \
 */
public class Util {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordProducer.class);

    public static String timestmp2date(Long timestamp){
        String str = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        str = sdf.format(date);
        return str;
    }

    public void consume() {


        KafkaConsumer consumer = KafkaHelper.getConsumer("consume-source-topic",
                Constant.STRING_DESERIALIZER, Constant.STRING_DESERIALIZER);
        consumer.subscribe(Arrays.asList(Constant.SOURCE_TOPIC));
        while (true) {
            ConsumerRecords<String, Long> records = consumer.poll(100);
            for (ConsumerRecord<String, Long> record : records) {
                LOGGER.info("offset={},time={},key={},value={}",
                        record.offset(),timestmp2date(record.timestamp()), record.key(),record.value());
//                System.out.printf("offset = %d, time=%s, key = %s,window_start_time=%s,=window_start=%d,window_end=%d, value = %d%n", record.offset(), timestmp2date(record.timestamp()), record.key().key(),
//                        timestmp2date(record.key().window().start()), record.key().window().start(), record.key().window().end(), record.value());
            }
        }
    }

    public static void main(String[] args) {

        new Util().consume();

//        long now = System.currentTimeMillis();
//        long start = (now/60/1000L-10)*60*1000L;
//        System.out.println("now:" + now);
//        for (int i=10;i>=1;i--){
//            System.out.println("start-" + i + ":" + (start+i*60*1000L));
//        }
//        System.out.println("start-0:" + start);
    }
}
