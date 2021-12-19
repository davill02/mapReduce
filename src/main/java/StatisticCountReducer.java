import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StatisticCountReducer extends Reducer<LongWritable, SumAndCountWritable, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<SumAndCountWritable> values, Context context) throws IOException, InterruptedException {
        double mean = 0;
        double n = 0;
        double var = 0;
        for (SumAndCountWritable value : values) {
            mean += value.getSum();
            n += value.getCount();
            var += value.getSquadSum();
        }
        mean /= n;
        var /= n;
        var -= mean * mean;
        Text result = new Text();
        result.set("[" + mean + "," + var + "]");
        context.write(key, result);
    }
}
