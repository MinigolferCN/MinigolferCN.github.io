package cn.edu.zjgsu;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherBean implements WritableComparable<WeatherBean> {
    private double temperature;
    private int month;
    private int year;

    // getter/setter 保持不变
    public double getTemperature() { return temperature; }
    public void setTemperature(double temperature) { this.temperature = temperature; }
    public int getMonth() { return month; }
    public void setMonth(int month) { this.month = month; }
    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(temperature);
        dataOutput.writeInt(month);
        dataOutput.writeInt(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        temperature = dataInput.readDouble();
        month = dataInput.readInt();
        year = dataInput.readInt();
    }

    // 按月份升序、同月份温度降序排序（原有逻辑正确）
    @Override
    public int compareTo(WeatherBean o) {
        int r1 = Integer.compare(this.month, o.month);
        if (r1 == 0) {
            int r2 = Double.compare(this.temperature, o.temperature);
            return -r2; // 负号表示降序
        }
        return r1;
    }

    // 补充 toString 方便调试
    @Override
    public String toString() {
        return "WeatherBean{" +
                "temperature=" + temperature +
                ", month=" + month +
                ", year=" + year +
                '}';
    }

    public WeatherBean() {}
}
