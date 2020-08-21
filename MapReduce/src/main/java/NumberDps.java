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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * #1 MR app
 * Input: Google׳s syntactic Biarcs data set
 * Map - create all dependency paths between any 2 nouns (nouns are stemmed)
 * Reduce - outputs pair to dp+ngram but only for dps which occurs at least DPMIN times in different pairs
 * arguments: 0- input path, 1-output path, 2-DPMIN
 */
public class NumberDps {
    public static String COUNTER_TAG = "!";
    public static String VALUES_TAG = "*";

    public static String removeTag(Text taggedKey) {
        String res = taggedKey.toString();
        if (res.startsWith(COUNTER_TAG)) {
            res = res.replaceFirst(Pattern.quote(COUNTER_TAG), ""); // remove COUNTER_TAG
        } else if (res.startsWith(VALUES_TAG)) {
            res = res.replaceFirst(Pattern.quote(VALUES_TAG), ""); // remove VEC_SIZE_TAG
        }
        return res.trim();
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return partitionForValue(key, numPartitions);
        }

        public static int partitionForValue(Text key, int numPartitions) {
            if (key.toString().startsWith(COUNTER_TAG)) {
                return (Integer.valueOf(removeTag(key)).hashCode() & Integer.MAX_VALUE) % numPartitions;
            } else {
                return (removeTag(key).hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private long[] counters;
        int numOfReducers;

        public void setup(Context context) {
            numOfReducers = context.getNumReduceTasks();
            counters = new long[numOfReducers];
        }

        // input: ceil	post
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            context.write(gram, new Text(""));
            counters[PartitionerClass.partitionForValue(gram, numOfReducers)]++;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int c = 0; c < counters.length - 1; c++) {
                if (counters[c] > 0) {
                    context.write(new Text(COUNTER_TAG + (c + 1)), new Text(String.valueOf(counters[c])));
                }
                counters[c + 1] += counters[c];
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private String currentKey;
        private MultipleOutputs<Text, Text> mo;
        public static long initialOffset = 0;
        private long uniqueDPId;

        public void setup(Context context) {
            mo = new MultipleOutputs<>(context);
            currentKey = "";
            uniqueDPId = -1;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().startsWith(COUNTER_TAG)) {
                long offsetsSum = 0;
                for (Text value : values) {
                    offsetsSum += Long.parseLong(value.toString());
                }
                initialOffset = offsetsSum;
                return;
            }
            // key changed
            String newKey = removeTag(key);
            if (!newKey.equals(currentKey)) {
                uniqueDPId++;
                currentKey = newKey;
            }
            String dpId = String.valueOf(initialOffset + uniqueDPId);
            mo.write(new Text(newKey), new Text(dpId), "dpsToId/dpToId");
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mo.write(new Text("vec_size"), new Text(String.valueOf(initialOffset + uniqueDPId + 1)),
                    "vecSizes/vecSize");
            mo.close();
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Number dps");
        job.setJarByClass(NumberDps.class);
        job.setMapperClass(NumberDps.MapperClass.class);
        job.setPartitionerClass(NumberDps.PartitionerClass.class);
        job.setReducerClass(NumberDps.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //TextInputFormat
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path biarcsInput = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, biarcsInput);

        // multiple outputs
        MultipleOutputs.addNamedOutput(job, "dpsToId", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "vecSizes", TextOutputFormat.class,
                Text.class, Text.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
