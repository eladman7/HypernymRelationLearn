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
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * #3 MR app
 * Input: labeled pairs set and output of FilterAllDpsByDpmin
 * Map - write key as is - stem pair from annotated set
 * Reduce - write as is while ignores unlabeled pairs
 * arguments: 0- FilterAllDpsByDpmin_out path, 1- labeled pairs path, 2-output path
 */
public class JoinPairLabel {
    public static String FIRST_TAG = "#1";
    public static String SECOND_TAG = "#2";
    public static String VEC_SIZE_TAG = "!";

    public static String removeTag(Text taggedKey) {
        String res = taggedKey.toString();
        if (res.endsWith(FIRST_TAG)) {
            res = res.substring(0, res.length() - FIRST_TAG.length()); // remove First_TAG
        } else if (res.endsWith(SECOND_TAG)) {
            res = res.substring(0, res.length() - SECOND_TAG.length()); // remove SECOND_TAG
        } else if (res.startsWith(VEC_SIZE_TAG)) {
            res = res.replaceFirst(Pattern.quote(VEC_SIZE_TAG), ""); // remove VEC_SIZE_TAG
        }
        return res.trim();
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private boolean publishedVecSize;
        private String vec_size;

        public void setup(Context context) {
            publishedVecSize = false;
        }

        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splittedGram = gram.toString().split("\\s+");
            if (splittedGram.length == 2) {
                if (!publishedVecSize) {
                    vec_size = splittedGram[1];
                    publishedVecSize = true;
                }
            }
            // labeled pair table e.g <dog animal true>
            else if (splittedGram.length == 3) {
                context.write(new Text(LingusticUtils.stem(splittedGram[0]) + "\t" +
                        LingusticUtils.stem(splittedGram[1]) + FIRST_TAG), new Text(splittedGram[2]));
            }
            // first MR out e.g <dog	animal	0:dp1:NGRAM1>
            else {
                String value = gram.toString().replaceFirst(Pattern.quote(splittedGram[0]), "")
                        .replaceFirst(Pattern.quote(splittedGram[1]), "").trim();
                context.write(new Text(splittedGram[0] + "\t" + splittedGram[1] + SECOND_TAG), new Text(value));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (publishedVecSize) {
                writeVecSizeToAllReducers(context);
            }
        }

        private void writeVecSizeToAllReducers(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < context.getNumReduceTasks(); i++) {
                context.write(new Text(VEC_SIZE_TAG + i), new Text(vec_size));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private boolean gotLabeled;
        private String currentKey;
        private int vec_size;

        public void setup(Context context) {
            currentKey = "";
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().startsWith(VEC_SIZE_TAG)) {
                vec_size = Integer.parseInt(values.iterator().next().toString());
                return;
            }
            if (!removeTag(key).equals(currentKey)) {
                currentKey = removeTag(key);
                gotLabeled = false;
            }
            if (key.toString().endsWith(FIRST_TAG)) {
                Iterator<Text> iterator = values.iterator();
                Text next = iterator.next();
                gotLabeled = true;
                context.write(new Text(removeTag(key)), next);
            } else if (key.toString().endsWith(SECOND_TAG)) {
                // ignoring unlabeled pairs
                if (!gotLabeled) return;
                String[] split;
                String val;
                for (Text value : values) {
                    split = value.toString().split(Pattern.quote(":"));
                    val = value.toString().replaceFirst(Pattern.quote(split[0]),
                            addPadding(Integer.parseInt(split[0]), String.valueOf(vec_size).length()));
                    context.write(new Text(removeTag(key)), new Text(val));
                }
            }
        }

        private String addPadding(int num, int numOfDigits) {
            String s = String.valueOf(num);
            int count = 0;
            while (num != 0) {
                // num = num/10
                num /= 10;
                ++count;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < numOfDigits - count; i++) {
                sb.append(0);
            }
            if (!s.equals("0")) sb.append(s);
            return sb.toString();
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            if (key.toString().startsWith(VEC_SIZE_TAG)) {
                return (Integer.valueOf(removeTag(key)).hashCode() & Integer.MAX_VALUE) % numPartitions;
            } else {
                return (removeTag(key).hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Join pair label");
        job.setJarByClass(JoinPairLabel.class);
        job.setMapperClass(JoinPairLabel.MapperClass.class);
        job.setPartitionerClass(JoinPairLabel.PartitionerClass.class);
        job.setReducerClass(JoinPairLabel.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setCombinerClass(JoinPairLabel.ReducerClass.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path out2Input = new Path(args[0]);
        Path labeledPairsInput = new Path(args[1]);
        Path max_vec_size = new Path(args[2]);

        Path outputPath = new Path(args[3]);

        // SequenceFileInputFormat, TextInputFormat
        MultipleInputs.addInputPath(job, out2Input, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, labeledPairsInput, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, max_vec_size, TextInputFormat.class, MapperClass.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
