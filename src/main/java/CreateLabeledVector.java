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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

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
        return new Text(taggedKey.toString().replace(FIRST_TAG, ""));
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
                String[] indexToCountPair = splittedGram[2].split(",");
                String key = splittedGram[0] + "\t" + splittedGram[1] + ":" + indexToCountPair[0];
                String value = indexToCountPair[1];
                context.write(new Text(key), new Text(value));
            }
        }
    }

    // todo: memory assumption - all vector can be kept in memory before it is written
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private Boolean currentLabel;
        private String currentPair;
        //        private long vec_size;
        private StringBuilder vectorStr;

        public void setup(Context context) {
            currentLabel = null;
            currentPair = "";
            vectorStr = new StringBuilder();
//            vec_size = Integer.parseInt(context.getConfiguration().get(VEC_SIZE_NAME));
        }

        // input: <pair dp_index, count>
        // output: <[2,0,1], true>
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyStr = removeTag(key).toString();
            String pair = extractPairFromKey(keyStr);
            if (!pair.equals(currentPair)) {
                if (StringUtils.isNotEmpty(currentPair)) {
                    // write all vector
                    writeVector(context);
                }
                currentPair = pair;
                vectorStr = new StringBuilder();
                currentLabel = null;
            }
            if (key.toString().contains(FIRST_TAG)) {
                currentLabel = Boolean.parseBoolean(values.iterator().next().toString());
                vectorStr.append("[");
            } else {
                vectorStr.append(values.iterator().next().toString());
                vectorStr.append(", ");
            }
        }

        private void writeVector(Context context) throws IOException, InterruptedException {
            vectorStr.delete(vectorStr.length() - 2, vectorStr.length());
            vectorStr.append("]");
            context.write(new Text(vectorStr.toString()), new Text(String.valueOf(currentLabel)));
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            writeVector(context);
        }
        private String extractPairFromKey(String key) {
            String[] split = key.split("\\s+");
            return split[0] + "\t" + split[1].split(":")[0];
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
//        List<String> strings = Files.readAllLines(Paths.get(args[1]));
//        conf.set(VEC_SIZE_NAME, strings.get(0).split("\\s+")[1]);
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