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

    public static Text removeTag(Text taggedKey) {
        return new Text(taggedKey.toString().replace(FIRST_TAG, "").replace(SECOND_TAG, ""));
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splittedGram = gram.toString().split("\\s+");
            // labeled pair table e.g <dog animal true>
            if (splittedGram.length == 3) {
                context.write(new Text(LingusticUtils.stem(splittedGram[0]) + "\t" +
                        LingusticUtils.stem(splittedGram[1]) + FIRST_TAG), new Text(splittedGram[2]));
            }
            // first MR out e.g <dog	animal	0:X/NN/prep/2-like/VB/xcomp/1-Y/NN/acomp/0:NGRAM1>
            else {
                String value = gram.toString().replaceFirst(splittedGram[0], "")
                        .replaceFirst(splittedGram[1], "").trim();
                context.write(new Text(splittedGram[0] + "\t" + splittedGram[1] + SECOND_TAG), new Text(value));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private boolean gotLabeled;
        private String currentKey;

        public void setup(Context context) {
            currentKey = "";
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (!removeTag(key).toString().equals(currentKey)) {
                currentKey = removeTag(key).toString();
                gotLabeled = false;
            }
            if (key.toString().contains(FIRST_TAG)) {
                Iterator<Text> iterator = values.iterator();
                Text next = iterator.next();
                gotLabeled = true;
                context.write(removeTag(key), next);
            } else if (key.toString().contains(SECOND_TAG)) {
                // ignoring unlabeled pairs
                if (!gotLabeled) return;
                for (Text value : values) {
                    context.write(removeTag(key), value);
                }
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

        Path outputPath = new Path(args[2]);

        // SequenceFileInputFormat, TextInputFormat
        MultipleInputs.addInputPath(job, out2Input, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, labeledPairsInput, TextInputFormat.class, MapperClass.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
