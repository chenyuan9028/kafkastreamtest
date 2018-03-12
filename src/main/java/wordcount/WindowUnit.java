package wordcount;


/**
 * \* Created by chenyuan
 * \* Date: 2018/3/9
 * \* Time: 上午11:13
 * \
 */

/**
 * 自定义的窗口单元
 * @param <K>  窗口计算中所用的key类型
 * @param <V>  窗口计算中的value类型
 */
public class WindowUnit<K,V> {

    private long start;  //窗口开始的时间

    private KeyValueWrapper<K,V> keyvalue ;

    public WindowUnit(long start, KeyValueWrapper keyvalue) {
        this.start = start;
        this.keyvalue = keyvalue;
    }


    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public KeyValueWrapper getKeyvalue() {
        return keyvalue;
    }

    public void setKeyvalue(KeyValueWrapper keyvalue) {
        this.keyvalue = keyvalue;
    }

    public K getKey(){
        return keyvalue.getKey();
    }

    public V getValue(){
        return keyvalue.getValue();
    }


}
