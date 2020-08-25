import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

public class MergeVectors {
    /***
     * Simple Map Reduce ->
     * Input: Folder with all the vectors files
     * Output: One file, contain all the vectors - concatenated.
     */
    public static Text removeTag(Text taggedKey) {
        String res = taggedKey.toString();
        if (res.startsWith(FIRST_TAG)) {
            res = res.replaceFirst(Pattern.quote(FIRST_TAG), ""); // remove FIRST_TAG
        } else if (res.startsWith(SECOND_TAG)) {
            res = res.replaceFirst(Pattern.quote(SECOND_TAG), ""); // remove FIRST_TAG
        }
        return new Text(res.trim());
    }

    public static String FIRST_TAG = "#1";
    public static String SECOND_TAG = "#2";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            if (gram.toString().startsWith("vec_size")) {
                // Special key so that this will arrive first.
                context.write(new Text(FIRST_TAG + "KEY"), new Text(gram.toString().split("\\s+")[1]));
            } else {
                context.write(new Text(SECOND_TAG + "KEY"), gram);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        /*
           @RELATION iris

           @ATTRIBUTE sepallength  NUMERIC
           @ATTRIBUTE sepalwidth   NUMERIC
           @ATTRIBUTE petallength  NUMERIC
           @ATTRIBUTE petalwidth   NUMERIC
           @ATTRIBUTE class        {Iris-setosa,Iris-versicolor,Iris-virginica}
        * */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals(FIRST_TAG + "KEY")) {
                double vector_sz = Double.parseDouble(values.iterator().next().toString());
                StringBuilder header = new StringBuilder("@RELATION vectors\n");
                for (int i = 0; i < vector_sz; i++) {
                    header.append("@ATTRIBUTE ").append("col").append(i).append(" NUMERIC").append("\n");
                }
                header.append("@ATTRIBUTE ").append("class").append(" {True, False}").append("\n");
                header.append("@DATA");
                context.write(new Text(header.toString()), new Text(""));
                return;
            }
            for (Text vector : values) {
                context.write(vector, new Text(""));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (removeTag(key).hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Merge Vectors");
        job.setJarByClass(MergeVectors.class);
        job.setMapperClass(MergeVectors.MapperClass.class);
        job.setPartitionerClass(MergeVectors.PartitionerClass.class);
        job.setReducerClass(MergeVectors.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[1]);
//      FileInputFormat.addInputPath(job, input1);
        MultipleInputs.addInputPath(job, input1, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, MapperClass.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
