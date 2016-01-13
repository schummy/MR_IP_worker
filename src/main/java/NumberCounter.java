
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by user on 11/16/15.
 */
public class NumberCounter {
    private static Logger logger;
    private static Text keyText = new Text();



    public static void setLogger(Logger logger) {
        NumberCounter.logger = logger;
    }

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Pattern p = Pattern.compile("-?\\d+");
            Matcher m = p.matcher(value.toString());
            while (m.find()) {
                keyText.set(m.group());
                context.write(keyText, one);
                logger.debug("Number found: {}", keyText.toString());
            }
        }
    }
    public static class MyReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
        private static IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int res = 0;
            for (IntWritable val:values) {
                res += val.get();
            }
            result.set(res);
            logger.debug("key={} result={}", key, result);
            context.write(key, result);
        }
    }

    public static class MyCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
        private static IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int res = 0;
            for (IntWritable val:values) {
                res += val.get();
            }
            result.set(res);
            logger.debug("key={} result={}", key, result);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        setLogger(LoggerFactory.getLogger(NumberCounter.class) );
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Number Counter");
        job.setJarByClass(NumberCounter.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] +
                DateTimeFormatter.ofPattern("_yyyyMMddHHmmss").format(LocalDateTime.now()).toString()));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
