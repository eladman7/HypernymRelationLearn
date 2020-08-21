import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * #2 MR app
 * Input: output of FilterAllDpsByDpmin
 * Map - write key as is
 * Combine - find max vec_size
 * Reduce - find max vec_size
 * arguments: 0- input path, 1-output path
 */
public class FindMaxVectorSize {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            context.write(new Text(gram.toString().split("\\s+")[0]),
                    new LongWritable(Long.parseLong(gram.toString().split("\\s+")[1])));
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long currentMax = 0;
            for (LongWritable value : values) {
                if (currentMax < value.get())
                    currentMax = value.get();
            }
            context.write(key, new LongWritable(currentMax));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Find max vector size");
        job.setJarByClass(FindMaxVectorSize.class);
        job.setMapperClass(FindMaxVectorSize.MapperClass.class);
        job.setPartitionerClass(FindMaxVectorSize.PartitionerClass.class);
        job.setReducerClass(FindMaxVectorSize.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setCombinerClass(FindMaxVectorSize.ReducerClass.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path biarcsInput = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, biarcsInput);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
