package cn.edu.zjgsu;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WeatherReducer extends Reducer<WeatherBean, Text, Text, NullWritable> {
    @Override
    protected void reduce(WeatherBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        for (Text value : values) {
            if (flag<=3){
                flag +=1;
                context.write(value, NullWritable.get());
            }else{
                break;
            }
        }
    }
}
