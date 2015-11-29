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

public class LaneCount{

    public static class TokenizerMapper extends Mapper<Object, Text, CompositeKeyWritable, Text>{

        private final static Text k = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split(",");
            String room = line[0].split(":")[0];

            CompositeKeyWritable k = new CompositeKeyWritable();
            k.time = Long.parseLong(line[1]);
            k.room = room;

            context.write(k,value);
        }
    }

    public static class IntSumReducer
            extends Reducer<CompositeKeyWritable,Text,Text,NullWritable> {
            private Text result = new Text();

            public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String motion = "null";
                for (Text val : values) {
                    String[] line = val.toString().split(","); 
                    if(line.length > 3){
                        result.set(val.toString() + "," + motion);
                        context.write(result, NullWritable.get());
                    }
                    else{
                        motion = line[2]; 
                    }
                }
            }
    }

    public static final class SortReducerByValuesPartitioner extends Partitioner<CompositeKeyWritable,Text> {
        public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
            return key.room.hashCode() % numPartitions;
        }
    }


    public static final class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
        private long time;
        private String room;

        @Override
        public void readFields(DataInput in) throws IOException {
            time = Long.parseLong(in.readUTF());
            room = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF("" + time);
            out.writeUTF(room);
        }

        @Override
        public int compareTo(CompositeKeyWritable key){
            return key.room.compareTo(room);
        }
    }

    public static final class CompositeKeyGroupingComparator extends WritableComparator {
        protected CompositeKeyGroupingComparator(){
            super(CompositeKeyWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            CompositeKeyWritable k1 = (CompositeKeyWritable) w1;
            CompositeKeyWritable k2 = (CompositeKeyWritable) w2;
            return k1.room.compareTo(k2.room);
        }
    }

    public static final class CompositeKeyOrderingComparator extends WritableComparator {
        protected CompositeKeyOrderingComparator(){
            super(CompositeKeyWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            CompositeKeyWritable k1 = (CompositeKeyWritable) w1;
            CompositeKeyWritable k2 = (CompositeKeyWritable) w2;
            int result = k1.room.compareTo(k2.room);
            if(result == 0) result = -1 * Long.compare(k1.time, k2.time);
            return result;

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Lane count");
        job.setJarByClass(LaneCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setSortComparatorClass(CompositeKeyOrderingComparator.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
        job.setPartitionerClass(SortReducerByValuesPartitioner.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPaths(job, "input/motion,input/switch");
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
