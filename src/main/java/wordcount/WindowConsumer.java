package wordcount;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by chenyuan
 * Date: 2018/3/7
 * Time: 下午2:04
 */

/**
 * 消费窗口流转换成的topic,进行第三级粒度的统计，比如返回最近10分钟／半小时／1小时数据
 */
public class WindowConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowConsumer.class);

    KafkaConsumer consumer;

    public WindowConsumer() {

        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer(Serdes.String().deserializer());

        consumer = KafkaHelper.getConsumer("consume-window-out-topic",
                windowedDeserializer, Constant.LONG_DESERIALIZER);
    }

    public Consumer getConsumer() {
        return consumer;
    }


    public void consume() {

        consumer.subscribe(Arrays.asList(Constant.WINDOW_OUTPUT_TOPIC));
        while (true) {
            ConsumerRecords<Windowed<String>, Long> records = consumer.poll(100);
            for (ConsumerRecord<Windowed<String>, Long> record : records) {
                LOGGER.info("offset={},time={},key={},window_start_time={},window_start={},window_end={},value={}",
                        record.offset(),Util.timestmp2date(record.timestamp()), record.key().key(),
                        Util.timestmp2date(record.key().window().start()),record.key().window().start(), record.key().window().end(),
                        record.value());
//                System.out.printf("offset = %d, time=%s, key = %s,window_start_time=%s,=window_start=%d,window_end=%d, value = %d%n", record.offset(), timestmp2date(record.timestamp()), record.key().key(),
//                        timestmp2date(record.key().window().start()), record.key().window().start(), record.key().window().end(), record.value());
            }
        }
    }

    /**
     * @param interval 窗口的个数，比如窗口时间为1分钟，要查最近10分钟内的数据，那么interval就等于9(=10-1)
     */
    public void poll(long interval) {
        long now = System.currentTimeMillis();
        long end = (now / 60 / 1000L) * 60 * 1000L;
        LOGGER.info("now time:{}",Util.timestmp2date(now));
        long start = (now / 60 / 1000L - interval) * 60 * 1000L;

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(Constant.WINDOW_OUTPUT_TOPIC);
        List<TopicPartition> partitions = new ArrayList<>();
        partitionInfos.stream().forEach(e -> partitions.add(new TopicPartition(e.topic(), e.partition())));

        consumer.assign(partitions);
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        partitions.stream().forEach(e -> timestampsToSearch.put(e, start));
        //获取每个分区消费的开始时间戳
        Map<TopicPartition, OffsetAndTimestamp> mapOffset = consumer.offsetsForTimes(timestampsToSearch);
        //指定每个分区消费的开始时间戳
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : mapOffset.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue().offset());
            } else {
                LOGGER.warn("seekOffset failed!");
                return;
            }
        }
        List<WindowUnit> windows = new ArrayList<>();
        Map<Long, Map<String, Long>> group = new HashMap<>();
        long latest = 0L;
        //当获取的记录的窗口开始时间时间大于查询时的当前时间时退出循环
        while (latest <= end) {
            ConsumerRecords<Windowed<String>, Long> records = consumer.poll(100);
            for (ConsumerRecord<Windowed<String>, Long> record : records) {
                //获取每条记录的窗口开始时间
                latest = record.key().window().start();
//                LOGGER.info("record_timstamp:{},record time:{}",record.timestamp(),Util.timestmp2date(latest));
                if (latest <= end) {
                    //将窗口topic中的记录转成WindowUnit对象并添加进列表中
                    windows.add(new WindowUnit<String, Long>(latest, new KeyValueWrapper<>(record.key().key(), record.value())));
                } else {
                    //当记录的窗口开始时间大于查询时的时间戳时，消费结束，进行数据处理
                    windows.stream().forEach(e -> {
                        if (!group.containsKey(e.getStart())) {
                            Map<String, Long> kv = new HashMap<>();
                            kv.put((String) e.getKey(), (Long) e.getValue());
                            group.put(e.getStart(), kv);
                        } else {
                            group.get(e.getStart()).put((String) e.getKey(), (Long) e.getValue());
                        }
                    });

                    break;
                }
            }
        }

        Map<String, Long> result = new HashMap<>();
        group.values().stream().flatMap(r -> r.entrySet().stream()).forEach(
                e -> {
                    if (!result.containsKey(e.getKey())) {
                        result.put(e.getKey(), e.getValue());
                    } else {
                        long oldValue = result.get(e.getKey());
                        result.put(e.getKey(), e.getValue() + oldValue);
                    }
                }
        );

        result.entrySet().stream().sorted((a1, a2) -> -a1.getValue().compareTo(a2.getValue()))
                .forEach(e -> LOGGER.info("key={}; value={}" ,e.getKey(), e.getValue()));


    }

    public static void main(String[] args) {
//        new WindowConsumer().consume();

            long start = System.currentTimeMillis();
            new WindowConsumer().poll(60);
            long end = System.currentTimeMillis();
            long time = (end - start) / 1000L;
            LOGGER.info("耗时：{}s" , time );



    }
}
