package wordcount;


import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 *  Created by chenyuan
 *  Date: 2018/3/8
 *  Time: 上午10:25
 */

/**
 * 测试类，输出中间topic
 */
public class TestMidTopic {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestMidTopic.class);

    public static String timestmp2date(Long timestamp){
        String str = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        str = sdf.format(date);
        return str;
    }


    public static void consumeMidTopic(){
        Consumer<String,Long> consumer = new KafkaConsumer<String, Long>(KafkaHelper.getConsumerProps("consume-mid-topic"),
                Constant.STRING_DESERIALIZER,Constant.LONG_DESERIALIZER);
        consumer.subscribe(Arrays.asList(Constant.MID_TOPIC));
        while(true){
            ConsumerRecords<String,Long> records = consumer.poll(1000);
            for (ConsumerRecord<String,Long> record:records){
                LOGGER.info("offset={},time={},timestamp={},key={},value={}",record.offset(),
                        timestmp2date(record.timestamp()),record.timestamp(),record.key(),record.value());
//                System.out.printf("offset = %d,time=%s,timestamp=%d,key=%s,value=%d%n",record.offset()
//                ,timestmp2date(record.timestamp()),record.timestamp(),record.key(),record.value());
            }
        }
    }

    public static void main(String[] args) {
        consumeMidTopic();
    }
}
