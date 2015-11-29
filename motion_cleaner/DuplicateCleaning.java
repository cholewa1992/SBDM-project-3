import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DuplicateCleaning{

    public static class DuplicateMapper extends Mapper<Object, Text, Text, NullWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
                context.write(value,NullWritable.get());
        }
    }

    public static class DuplicateReducer
            extends Reducer<Text,NullWritable,Text,NullWritable> {

            public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                    context.write(key, NullWritable.get());
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Motion data cleaning");
        job.setJarByClass(DuplicateCleaning.class);

        job.setMapperClass(DuplicateMapper.class);
        job.setReducerClass(DuplicateReducer.class);
        job.setCombinerClass(DuplicateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPaths(job, "input/motion");
        FileOutputFormat.setOutputPath(job, new Path("input/motion_clean"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
