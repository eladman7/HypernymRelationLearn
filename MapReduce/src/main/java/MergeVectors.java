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

public class MergeVectors {
    /***
     * Simple Map Reduce ->
     * Input: Folder with all the vectors files
     * Output: One file, contain all the vectors - concatenated.
     */

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            context.write(new Text("KEY"), gram);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String vecKey, vecVal;
            int lastCommaIndex;
            for (Text vector : values) {
                lastCommaIndex = vector.toString().lastIndexOf(",");
                vecKey = vector.toString().substring(0, lastCommaIndex + 1).trim();
                vecVal = vector.toString().substring(lastCommaIndex + 1).trim();
                context.write(new Text(vecKey), new Text(vecVal));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() & Integer.MAX_VALUE % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Merge Vectors");
        job.setJarByClass(MergeVectors.class);
        job.setMapperClass(MergeVectors.MapperClass.class);
        job.setPartitionerClass(MergeVectors.PartitionerClass.class);
        job.setReducerClass(MergeVectors.ReducerClass.class);
        job.setCombinerClass(MergeVectors.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path input1 = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, input1);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
