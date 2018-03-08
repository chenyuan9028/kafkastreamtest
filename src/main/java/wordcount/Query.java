package wordcount;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.kstream.Windowed;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/8
 * \* Time: 上午10:25
 * \
 */
public class Query {

    public String timestmp2date(Long timestamp){
        String str = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        str = sdf.format(date);
        return str;
    }
    public void query(){
        WindowConsumer windowConsumer = new WindowConsumer();
        Consumer consumer = windowConsumer.getConsumer();
        long start = (System.currentTimeMillis()/60*1000L)*60*1000L;
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(Constant.WINDOW_OUTPUT_TOPIC);
        List<TopicPartition> partitions = new ArrayList<>();
        partitionInfos.stream().forEach(e->partitions.add(new TopicPartition(e.topic(),e.partition())));
        consumer.assign(partitions);
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        partitions.stream().forEach(e->timestampsToSearch.put(e,start));
        Map<TopicPartition, OffsetAndTimestamp> mapOffset = consumer.offsetsForTimes(timestampsToSearch);
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : mapOffset.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue().offset());
            } else {
                System.err.println("seekOffset failed!");
                return;
            }
        }
        int trytimes = 0;
        while (trytimes<4){
            ConsumerRecords<Windowed<String>, Long> records = consumer.poll(100);
            if(records.count()==0){
                trytimes += 1;
            }else{
                for (ConsumerRecord<Windowed<String>, Long> record : records){
                    System.out.printf("offset = %d, time=%s, key = %s,window_start=%d,window_end=%d, value = %d%n", record.offset(),timestmp2date(record.timestamp()), record.key().key(),
                            record.key().window().start(),record.key().window().end(),record.value());
                }
            }

        }


    }
}
