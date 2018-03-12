package wordcount;

import org.apache.kafka.streams.KeyValue;

/**
 * \* Created by chenyuan
 * \* Date: 2018/3/9
 * \* Time: 下午1:36
 * \
 */

/**
 * 自定义的KeyValue封装类，拓展了原有的KeyValue,提供了getKey()和getValue()方法
 * @param <K>
 * @param <V>
 */
public class KeyValueWrapper<K,V> {

    public final K key;

    public final V value;


    KeyValue keyValue;

    public KeyValueWrapper(final K key, final V value) {
        this.key = key;
        this.value = value;
        this.keyValue = KeyValue.pair(key,value);
    }

    public K getKey(){
        return key;
    }

    public V getValue(){
        return value;
    }
}
