package cn.edu.zjgsu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MyReducer extends Reducer<Text, Text, Text, Integer> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Integer>.Context context) throws IOException, InterruptedException {
        Map<String, Integer> fileCountMap = new HashMap<>();

        for  (Text value : values) {
            String filename = value.toString();
            fileCountMap.put(filename,fileCountMap.getOrDefault(filename,0) + 1);
        }
        for (Map.Entry<String, Integer> entry : fileCountMap.entrySet()) {
            String filename = entry.getKey();
            Integer count = entry.getValue();
            context.write(new Text(key + "--" + filename), count);
        }
    }
}
