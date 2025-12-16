package cn.edu.zjgsu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String word = value.toString().split("--")[0];
        String docnum = value.toString().split("--")[1];
        String doc = docnum.split("\t")[0];
        String num = docnum.split("\t")[1];
        context.write(new Text(word), new Text(doc + "-->" + num));
    }
}
