import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class StatisticCountMapper extends Mapper<Object, Text, LongWritable, SumAndCountWritable> {
    private Map<Long, List<Double>> keyStats;

    @Override
    public void setup(Context context) throws IOException {
        keyStats = new HashMap<Long, List<Double>>();
    }

    @Override
    public void map(Object key, Text input, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(input.toString());
        while (tokenizer.hasMoreElements()) {
            Long keyValue = Long.parseLong(tokenizer.nextToken());
            Double value = Double.parseDouble(tokenizer.nextToken());
            if (keyStats.containsKey(keyValue)) {
                List<Double> stats = keyStats.get(keyValue);
                stats.set(0, stats.get(0) + 1);
                stats.set(1, stats.get(1) + value);
                stats.set(2, stats.get(2) + value * value);
            } else {
                List<Double> stats = new ArrayList<Double>();
                stats.add(1.);
                stats.add(value);
                stats.add(value * value);
                keyStats.put(keyValue, stats);
            }
        }

        for (Map.Entry<Long, List<Double>> entry : keyStats.entrySet()) {
            LongWritable keyValue = new LongWritable();
            keyValue.set(entry.getKey());
            List<Double> stats = entry.getValue();
            context.write(keyValue, new SumAndCountWritable(stats.get(0), stats.get(1), stats.get(2)));
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        keyStats.clear();
    }
}
