package wordcount;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *  Created by chenyuan
 *  Date: 2018/3/8
 *  Time: 下午2:38
 */

/**
 * 消费原始topic内容生成初级粒度的中间topic,比如每30秒向中间topic输入一次，则累计最近30秒的数据，处理完后输出一条
 * 记录至中间topic
 */
public class MidTopic {

    /**
     *
     * @param value  原始topic记录中的value
     * @return  返回处理的结果
     */
    public static String processRecord(String value){
        String[] arr = value.split("\\|");
        int length = arr.length;
        return arr[length -1];
    }


    /**
     * 每隔一定的时间产生一条记录生成中间topic
     * @param interval 初级粒度的间隔时间(单位：秒)
     */
    public static void genMidTopic(long interval) {

        Consumer<String,String> consumer = new KafkaConsumer<>(KafkaHelper.getConsumerProps("test-group-4"),
                new StringDeserializer(),new StringDeserializer());
        consumer.subscribe(Arrays.asList(Constant.SOURCE_TOPIC));
        Map<String,Long> count = new HashMap<>();
        String key ;
        //或得大于等于当前时间戳的最近间隔的时间戳
        Long latest = 0L;
        KafkaProducer<String,Long> producer = new KafkaProducer<>(KafkaHelper.getProducerProps(),Constant.STRING_SERIALIZER,
                Constant.LONG_SERIALIZER);
        int i = 0;
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record:records){
                long timestamp = record.timestamp();
                if(i == 0){
                    latest = (timestamp/interval/1000L+1)*interval*1000L;
                    i++;
                }
                if(timestamp>latest){
                    final long temp = latest - interval*1000L + 100L;
                    count.entrySet().stream().forEach(
                            e->producer.send(new ProducerRecord<String, Long>(Constant.MID_TOPIC,null,temp,e.getKey(),e.getValue())));
                    latest += interval*1000L;
                    count.clear();
                    count.put(processRecord(record.value()),1L);
                }else {
                    if(timestamp>=latest-interval*1000L){
                        key = processRecord(record.value());
                        if(count.get(key) == null) {
                            count.put(key,1L);
                        }else{
                            count.put(key,count.get(key)+1);
                        }
                    }
                }

            }
        }
    }

    public static void main(String[] args) {
        genMidTopic(Constant.FIRST_INTERVAL);
    }

}
