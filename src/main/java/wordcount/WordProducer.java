package wordcount;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created by chenyuan
 * Date: 2018/3/7
 * Time: 下午3:32
 */

/**
 * 生产者生成原始topic内容
 */
public class WordProducer {

    private final Producer producer;

    private static final Logger LOGGER = LoggerFactory.getLogger(WordProducer.class);

    private WordProducer(){

        producer = KafkaHelper.getProducer(Constant.STRING_SERIALIZER,Constant.STRING_SERIALIZER);

    }

    public static int getRandom(int min, int max){
        Random random = new Random();
        int s = random.nextInt(max) % (max - min + 1) + min;
        return s;

    }

    public void produce(){

        while (true){
            Long timestamp = System.currentTimeMillis();
            String key = String.valueOf(timestamp);
            String time = KafkaHelper.timestmp2date(timestamp);
            char a = (char) getRandom(97,122);
            char b = (char) getRandom(97,122);
            String value = String.valueOf(a) + "|" + String.valueOf(b);
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(Constant.SOURCE_TOPIC,key,value);
            producer.send(record);
            LOGGER.info("time={};key={};value={}",time,key,value);
//            System.out.println("time=" + time + "; key=" + key + "; value=" + value);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args){
        new WordProducer().produce();
    }
}
