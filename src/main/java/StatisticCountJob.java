import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayList;
import java.util.List;

public class StatisticCountJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();

        // Error check argument number, otherwise print out correct usage.
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage:lab <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "lab");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(StatisticCountJob.class);
        job.setMapperClass(StatisticCountMapper.class);
        job.setCombinerClass(StatisticCountReducer.class);
        job.setReducerClass(StatisticCountReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SumAndCountWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Check for if we skip anything.
        List<String> otherArgs = new ArrayList<>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("lab.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
