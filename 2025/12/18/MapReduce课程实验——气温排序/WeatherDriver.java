package cn.edu.zjgsu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeatherDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(),"weathersort");
        job.setJarByClass(WeatherDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.38.129:9000/input"));

        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(WeatherBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(weatherPartition.class);
        job.setNumReduceTasks(12);

        job.setGroupingComparatorClass(weatherGroupComparator.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.129:9000/output"));

        boolean flag = job.waitForCompletion(true);
        return flag ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WeatherDriver(), args);
        System.exit(exitCode);
    }
}
