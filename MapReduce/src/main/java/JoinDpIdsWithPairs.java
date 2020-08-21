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

/**
 * #3 MR app
 * Input: labeled pairs set and output of FilterAllDpsByDpmin
 * Map - write key as is - stem pair from annotated set
 * Reduce - write as is while ignores unlabeled pairs
 * arguments: 0- FilterAllDpsByDpmin_out path, 1- labeled pairs path, 2-output path
 */
public class JoinDpIdsWithPairs {
    public static String FIRST_TAG = "#1";
    public static String SECOND_TAG = "#2";

    public static Text removeTag(Text taggedKey) {
        String res = taggedKey.toString();
        if (res.endsWith(FIRST_TAG)) {
            res = res.replaceFirst(Pattern.quote(FIRST_TAG), ""); // remove FIRST_TAG
        } else if (res.endsWith(SECOND_TAG)) {
            res = res.replaceFirst(Pattern.quote(SECOND_TAG), ""); // remove SECOND_TAG
        }
        return new Text(res.trim());
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splittedGram = gram.toString().split("\\s+");
            // X/advcl-Y/nsubj	0
            if (splittedGram.length == 2) {
                context.write(new Text(splittedGram[0] + FIRST_TAG), new Text(splittedGram[1]));
            }
            // X/advmod-Y/acomp	insulin	resist:are	are/VBP/advcl/0 insulin/NN/advmod/3 resist/NN/acomp/1
            else {
                context.write(new Text(splittedGram[0] + SECOND_TAG),
                        new Text(gram.toString().replace(splittedGram[0], "").trim()));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private String currentKey;
        private int currentKeyIndex;
        private boolean gotIndex;

        public void setup(Context context) {
            gotIndex = false;
            currentKey = "";
            currentKeyIndex = -1;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (!removeTag(key).toString().equals(currentKey)) {
                currentKey = removeTag(key).toString();
            }
            if (key.toString().endsWith(FIRST_TAG)) {
                String firstVal = values.iterator().next().toString();
                currentKeyIndex = Integer.parseInt(firstVal);
                gotIndex = true;
            } else if (key.toString().endsWith(SECOND_TAG)) {
                // write somthing like that:
                // ceil	post	0:X/ccomp-Y/nsubj:are	ceil/NN/ccomp/2 post/NNS/nsubj/3 are/VBP/ROOT/0
                // from somthing like this:
                // X/advmod-Y/acomp	insulin	resist:are	are/VBP/advcl/0 insulin/NN/advmod/3 resist/NN/acomp/1
                // assuming 1 value
                if (!gotIndex) throw new IOException("Error: did not got index. dp: " + key.toString());
                String[] valSplit, secondSplit;
                String pair, ngram, curValue;
                for (Text value : values) {
                    valSplit = value.toString().split("\\s+");
                    secondSplit = valSplit[1].split(Pattern.quote(":"));
                    pair = valSplit[0] + "\t" + secondSplit[0];
                    ngram = value.toString().split(Pattern.quote(":"))[1];
                    curValue = currentKeyIndex + ":" + removeTag(key).toString() + ":" + ngram;
                    context.write(new Text(pair), new Text(curValue));
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
        Job job = new Job(conf, "Join dp ids and pairs");
        job.setJarByClass(JoinDpIdsWithPairs.class);
        job.setMapperClass(JoinDpIdsWithPairs.MapperClass.class);
        job.setPartitionerClass(JoinDpIdsWithPairs.PartitionerClass.class);
        job.setReducerClass(JoinDpIdsWithPairs.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setCombinerClass(JoinDpIdsWithPairs.ReducerClass.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path dpsToPair = new Path(args[0]);
        Path dpToIds = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // SequenceFileInputFormat, TextInputFormat
        MultipleInputs.addInputPath(job, dpsToPair, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, dpToIds, TextInputFormat.class, MapperClass.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}