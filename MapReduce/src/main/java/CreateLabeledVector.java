import org.apache.commons.lang.StringUtils;
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
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * #5 MR app
 * Input: CountDpsPerPair output
 * Map - <pair, [label]> stay the same. <pair, [(dp_index,count)]> mapped into <pair:dp_index, count>
 * ensure key with label comes first
 * Reduce - build a labeled vector for each pair
 * arguments: 0- CountDpsPerPair_out path, 1-output path
 */
public class CreateLabeledVector {
    public static String FIRST_TAG = "*";
//    public static String VEC_SIZE_NAME = "vec_size";

    public static Text removeTag(Text taggedKey) {
        String res = taggedKey.toString();
        if (res.endsWith(FIRST_TAG)) {
            res = res.substring(0, res.length() - 1); // remove FIRST_TAG
        } else {
//            word1\tword2:0
            res = res.split(Pattern.quote(":"))[0];
        }
        return new Text(res.trim());
    }

    public static boolean isLabelData(String data) {
        return data.toLowerCase().contains("false") || data.toLowerCase().contains("true");
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        /*
            chair	aircraft	false
            chair	aircraft	0,0
            chair	aircraft	1,0
        */
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splittedGram = gram.toString().split("\\s+");
            if (isLabelData(splittedGram[2])) {
                context.write(new Text(splittedGram[0] + "\t" + splittedGram[1] + FIRST_TAG), new Text(splittedGram[2]));
            } else {
                String[] indexToCountPair = splittedGram[2].split(Pattern.quote(","));
                String key = splittedGram[0] + "\t" + splittedGram[1] + ":" + indexToCountPair[0];
                String value = indexToCountPair[1];
                context.write(new Text(key), new Text(value));
            }
        }
    }

    // todo: memory assumption - all vector can be kept in memory before it is written
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private String currentLabel;
        private String currentPair;
        private StringBuilder vectorStr;

        public void setup(Context context) {
            currentLabel = null;
            currentPair = "";
            vectorStr = new StringBuilder();
        }

        // input: <pair dp_index, count>
        // output: <[2,0,1], true>
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyStr = removeTag(key).toString();
            String pair = keyStr;
            if (!keyStr.endsWith(FIRST_TAG)) {
                pair = extractPairFromKey(keyStr);
            }
            if (!pair.equals(currentPair)) {
                if (StringUtils.isNotEmpty(currentPair)) {
                    // write all vector
                    writeVector(context);
                }
                currentPair = pair;
                vectorStr = new StringBuilder();
                currentLabel = null;
            }
            if (key.toString().endsWith(FIRST_TAG)) {
                currentLabel = values.iterator().next().toString();
            } else {
                vectorStr.append(values.iterator().next().toString());
                vectorStr.append(",");
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            writeVector(context);
        }

        private void writeVector(Context context) throws IOException, InterruptedException {
            if (validVector(vectorStr.toString())) {
                context.write(new Text(currentPair), new Text(vectorStr.append(currentLabel).toString()));
            }
        }

        private boolean validVector(String vector) {
            boolean b = Arrays.stream(vector.split(Pattern.quote(",")))
                    .allMatch(x -> x.equals("0"));
            return !b;
        }

        private String extractPairFromKey(String key) {
            String[] split = key.split("\\s+");
            if (key.endsWith(FIRST_TAG)) {
                return split[0] + "\t" + split[1];
            }
            return split[0] + "\t" + split[1].split(Pattern.quote(":"))[0];
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
        Job job = new Job(conf, "Create labeled vector");
        job.setJarByClass(CreateLabeledVector.class);
        job.setMapperClass(CreateLabeledVector.MapperClass.class);
        job.setPartitionerClass(CreateLabeledVector.PartitionerClass.class);
        job.setReducerClass(CreateLabeledVector.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setCombinerClass(CreateLabeledVector.ReducerClass.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path out3Input = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // SequenceFileInputFormat, TextInputFormat
        FileInputFormat.addInputPath(job, out3Input);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
