package cn.edu.zjgsu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class WeatherMapper extends  Mapper<LongWritable, Text, WeatherBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WeatherBean, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split("\t");

        String timeStr = parts[0];
        String year = timeStr.substring(0, 4);
        String monthKey = timeStr.substring(5, 7);

        int month = Integer.parseInt(monthKey);
        double temperature = Double.parseDouble(parts[1].replace("c", "").trim());

        WeatherBean wb = new WeatherBean();
        wb.setMonth(month);
        wb.setTemperature(temperature);
        wb.setYear(Integer.parseInt(year));

        context.write(wb, value);


    }
}
