package wordcount;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/8
 * \* Time: 下午2:09
 * \
 */
public class KafkaHelper {

    public static Properties getConsumerProps(String group) {
        Properties props = new Properties();

        props.put("bootstrap.servers", Constant.BROKERS);
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");


        return props;
    }

    public static Properties getProducerProps() {
        Properties props = new Properties();

        props.put("bootstrap.servers",Constant.BROKERS);
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);

        return props;
    }


    public static String timestmp2date(Long timestamp){
        String str = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        str = sdf.format(date);
        return str;
    }

    public static <T,V> KafkaConsumer<? extends Deserializer<T>, ? extends Deserializer<V>>  getConsumer(String group,Deserializer<T> key, Deserializer<V> value) {
        KafkaConsumer consumer = new KafkaConsumer<>(getConsumerProps(group),key,value);
        return consumer;
    }

    public static <T,V> KafkaProducer<? extends Serializer<T>, ? extends Serializer<V>>  getProducer(Serializer<T> key, Serializer<V> value) {
        KafkaProducer producer = new KafkaProducer<>(KafkaHelper.getProducerProps(),key,value);
        return producer;
    }


}
