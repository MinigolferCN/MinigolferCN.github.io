package cn.edu.zjgsu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class weatherPartition extends Partitioner<WeatherBean, Text> {
    @Override
    public int getPartition(WeatherBean weatherBean, Text text, int i) {
        int month = weatherBean.getMonth() - 1;
        return month;
    }
}