package cn.edu.zjgsu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class weatherGroupComparator extends WritableComparator {
    // 构造方法增加 true 参数，启用实例化
    public weatherGroupComparator() {
        super(WeatherBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherBean first = (WeatherBean) a;
        WeatherBean second = (WeatherBean) b;
        // 直接比较月份整数值（核心修复点）
        return Integer.compare(first.getMonth(), second.getMonth());
    }
}
