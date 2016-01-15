import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Created by user on 1/12/16.
 */
public class IPWorker extends Configured implements Tool {
    private Logger logger;
    private String outputSeparator = ",";
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public int run(String[] strings) throws Exception {
        setLogger(LoggerFactory.getLogger(IPWorker.class));

        Configuration conf = getConf();


        Job job = Job.getInstance(conf, "IP parser");
        setTextoutputformatSeparator(job, outputSeparator);
        setCompressionProperties(job);

        job.setNumReduceTasks(1);

        job.setJarByClass(IPMapper.class);
        job.setMapperClass(IPMapper.class);
        job.setCombinerClass(IPCombiner.class);
        job.setReducerClass(IPReducer.class);

        // map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCountAverage.class);

        // reducer output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumCountAverage.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1] +
                //DateTimeFormatter.ofPattern("_yyyyMMddHHmmss").format(LocalDateTime.now()).toString()));
                System.currentTimeMillis() ) );
        return job.waitForCompletion(true)? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IPWorker(), args);
        System.exit(res);

    }

    protected static void setTextoutputformatSeparator(final Job job, final String separator){
        final Configuration conf = job.getConfiguration(); //ensure accurate config ref

        conf.set("mapred.textoutputformat.separator", separator); //Prior to Hadoop 2 (YARN)
        conf.set("mapreduce.textoutputformat.separator", separator);  //Hadoop v2+ (YARN)
        conf.set("mapreduce.output.textoutputformat.separator", separator);
        conf.set("mapreduce.output.key.field.separator", separator);
        conf.set("mapred.textoutputformat.separatorText", separator); // ?


    }
    protected static void setCompressionProperties(final Job job) {
        final Configuration conf = job.getConfiguration(); //ensure accurate config ref

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);

        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.SnappyCodec");
    }

    public static class IPMapper extends Mapper<LongWritable, Text, Text, SumCountAverage>{

        private Text ip = new Text();
        private SumCountAverage zeroSize = new SumCountAverage(0, 1);
        private SumCountAverage sumCount = new SumCountAverage(0, 1);
        private Logger logger;


        public static final int NUM_FIELDS = 9;

        @Override
        protected void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
          //  logger.info("map started");

            String logEntryPattern = "^(ip[\\d]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+) \"([^\"]*)\" \"([^\"]+)\"";            String[] tokens;

            Pattern p = Pattern.compile(logEntryPattern);
            Matcher matcher = p.matcher(value.toString());
            if (!matcher.matches() ||
                    NUM_FIELDS != matcher.groupCount()) {
                logger.warn("Bad log entry:\n {}", value.toString());
                return;
            }
            ip.set(matcher.group(1));

            if (StringUtils.isNumeric( matcher.group(7)) ) {
                sumCount.setSum(Integer.parseInt(matcher.group(7)));
                context.write(ip, sumCount);
            } else {
                context.write(ip, zeroSize);
            }

            //context.write(ip, intWritable);

            /*
            logger.debug("IP Address: {}", matcher.group(1));
            logger.debug("Date&Time: {}", matcher.group(4));
            logger.debug("Request: {}", matcher.group(5));
            logger.debug("Response: {}", matcher.group(6));
            logger.debug("Bytes Sent: {}", matcher.group(7));
            if (!matcher.group(8).equals("-"))
                logger.debug("Referer: {}", matcher.group(8));
                */
        }
    }
    public static class IPCombiner extends Reducer<Text, SumCountAverage, Text, SumCountAverage> {
        private static SumCountAverage result = new SumCountAverage();
        @Override
        protected void reduce(Text key, Iterable<SumCountAverage> values, Context context) throws IOException, InterruptedException {
           // logger.info("Combiner reduce started");
            int sum = 0;
            int count = 0;
            for (SumCountAverage val:values) {
                sum += val.getSum();
                count += val.getCount();
            }
            result.setSum(sum);
            result.setCount(count);

            context.write(key, result);
        }
    }
    public static class IPReducer extends Reducer<Text, SumCountAverage, Text, SumCountAverage> {
        private static SumCountAverage result = new SumCountAverage();
        @Override
        protected void reduce(Text key, Iterable<SumCountAverage> values, Context context) throws IOException, InterruptedException {
           //  logger.info("Reducer reduce started");

            int sum = 0;
            int count = 0;
            for (SumCountAverage val:values) {
                sum     += val.getSum();
                count   += val.getCount();
            }
            result.setSum(sum);
            result.setCount(count);
            context.write(key, result);

        }
    }


}
