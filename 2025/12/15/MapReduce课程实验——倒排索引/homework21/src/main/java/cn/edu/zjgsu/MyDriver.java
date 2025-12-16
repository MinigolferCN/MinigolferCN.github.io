package cn.edu.zjgsu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "MyDriver");
        job.setJarByClass(MyDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.38.129:9000/input"));

        job.setMapperClass(MyMapper.class); //
        job.setMapOutputKeyClass(Text.class); //  设定map阶段K2类型
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Integer.class);

        job.setOutputFormatClass(TextOutputFormat.class);   //  指定文件输出方式
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.129:9000/output"));  //  指定文件输出路径

        boolean b1 = job.waitForCompletion(true);   //  完成任务时返回1

        return b1 ? 0:1;
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        int run = ToolRunner.run(configuration, new MyDriver(), args);
        System.exit(run);   //  完成任务后结束进程
    }
}
