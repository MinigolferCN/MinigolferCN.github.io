package cn.edu.zjgsu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String docnum = "";
        for (Text value : values) {
            docnum = docnum + value.toString()+"\t";
        }
        context.write(key, new Text(docnum));
    }
}
