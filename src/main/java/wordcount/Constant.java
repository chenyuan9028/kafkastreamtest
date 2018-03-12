package wordcount;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/7
 * \* Time: 下午2:08
 * \
 */
public class Constant {

    public static final String BROKERS = "localhost:9092";

    public static final String SOURCE_TOPIC = "streams-test-input1";

    public static final String MID_TOPIC = "mid-stream1";

    public static final String WINDOW_OUTPUT_TOPIC = "windowed-ip-count4";

    public static final String OUTPUT_TOPIC = "ip-count";

    public static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

    public static final LongSerializer LONG_SERIALIZER = new LongSerializer();

    public static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

    public static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    //第一级粒度：消费原始topic每隔一定的时间输出一条记录生成中间topic，单位：秒
    public static final Long FIRST_INTERVAL = 30L;

    //第二级粒度：中间topic转换成流后进行窗口计算的窗口间隔，为FIRST_INTERVAL的整数倍，单位：秒
    public static final Long SECOND_INTERVAL = 30L;
}
