import org.apache.commons.lang.StringUtils;
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
 * #4 MR app
 * Input: JoinPairLabel output and vector size
 * Map - write key as is
 * Reduce - write as is while ignores unlabeled pairs
 * arguments: 0- JoinPairLabel_out path, 1- vec_size path, 2-output path
 */
public class CountDpsPerPair {
    /*
    get this
        <dog, animal>, 0<x like y>:ngram1
        <dog, animal>, 2<x as y>:ngram5
        <dog, animal>, 0<x like y>:ngram2
        <dog, animal>, true
    map into this:
        <<dog, animal> 0:<x like y> ,[ ngram1]>
        <<dog, animal> 2:<x as y> ,[ngram5]>
        <<dog, animal> 0:<x like y>, [ngram2]>
        <<dog, animal>, true>
    * */
    public static String FIRST_TAG = "*";
    public static String VEC_SIZE_TAG = "!";

    public static String removeTag(Text taggedKey) {
        String res = taggedKey.toString();
        if (res.endsWith(FIRST_TAG)) {
            res = res.substring(0, res.length() - 1); // remove First_TAG
        } else if (res.startsWith(VEC_SIZE_TAG)) {
            res = res.replaceFirst(Pattern.quote(VEC_SIZE_TAG), ""); // remove VEC_SIZE_TAG
        } else {
            res = res.split(Pattern.quote(":"))[0];
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
            } else {
                // dog	animal	true
                String newVal, newKey;
                if (splittedGram.length == 3) {
                    newKey = splittedGram[0] + "\t" + splittedGram[1] + "\t" + FIRST_TAG;
                    newVal = splittedGram[2];
                }
                // dog	animal	0:X/NN/prep/2-like/VB/xcomp/1-Y/NN/acomp/0:animal	animal/NN/acomp/0 like/VB/xcomp/1 dog/NN/prep/2
                else {
                    String[] dpSplit = splittedGram[2].split(Pattern.quote(":"));
                    newKey = splittedGram[0] + "\t" + splittedGram[1] + ":" + dpSplit[0];
                    newVal = dpSplit[1] + ":" + dpSplit[2];
                }
                context.write(new Text(newKey), new Text(newVal));
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

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            if (key.toString().startsWith(VEC_SIZE_TAG)) {
                return (Integer.valueOf(removeTag(key)).hashCode() & Integer.MAX_VALUE) % numPartitions;
            } else if (key.toString().endsWith(FIRST_TAG)) {
                return (removeTag(key).hashCode() & Integer.MAX_VALUE) % numPartitions;
            } else {
                // key looks like that: zurich	basl	36:X/conj-Y/nsubj
                String[] splittedGram = key.toString().split(Pattern.quote(":"));
                return (splittedGram[0].hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }
    }

    /*
            <<dog, animal>*, true>
            <<dog, animal> 0:<x like y> ,[ ngram1, ngram2]>
            <<dog, animal> 2:<x as y> ,[ngram5]>
            write ->
                Write(<<dog,animal>, [(0,2)]>)
                Write(<<dog,animal>, [(1,0)]>) -> because of the gap between last index 0 and current index 2
                Write(<<dog,animal>, [(2,1)]>)
                Write(<<dog, animal>, true>)
        */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private String currentPair;
        private int lastIndexOfPair;
        private long counterOfIndexInPair;
        private int vec_size;

        public void setup(Context context) {
            currentPair = "";
            lastIndexOfPair = -1;
            counterOfIndexInPair = 0;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().startsWith(VEC_SIZE_TAG)) {
                vec_size = Integer.parseInt(values.iterator().next().toString());
                return;
            }
            String pair = removeTag(key);
            if (!pair.equals(currentPair)) {
                // fill gap from last index to vec size
                if (StringUtils.isNotEmpty(currentPair)) {
                    if (lastIndexOfPair + 1 < vec_size) {
                        for (int i = lastIndexOfPair + 1; i < vec_size; i++) {
                            context.write(new Text(currentPair), new Text(
                                    addPadding(i, String.valueOf(vec_size).length()) + "," + "0"));
                        }
                    }
                }
                currentPair = pair;
                counterOfIndexInPair = 0;
                lastIndexOfPair = -1;
            }
            if (key.toString().endsWith(FIRST_TAG)) {
                Text next = values.iterator().next();
                context.write(new Text(currentPair), next);
            } else {
                int index = extractDpIndexFromKey(key.toString());
                // fill internal gaps of vector
                if (lastIndexOfPair + 1 < index) {
                    for (int i = lastIndexOfPair + 1; i < index; i++) {
                        context.write(new Text(currentPair),
                                new Text(addPadding(i, String.valueOf(vec_size).length()) + "," + "0"));
                    }
                }
                // continue with current index
                for (Text value : values) {
                    counterOfIndexInPair++;
                }
                context.write(new Text(currentPair),
                        new Text(addPadding(index, String.valueOf(vec_size).length())
                                + "," + String.valueOf(counterOfIndexInPair)));
                lastIndexOfPair = index;
                counterOfIndexInPair = 0;
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (lastIndexOfPair + 1 < vec_size) {
                for (int i = lastIndexOfPair + 1; i < vec_size; i++) {
                    context.write(new Text(currentPair),
                            new Text(addPadding(i, String.valueOf(vec_size).length()) + "," + "0"));
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

        private int extractDpIndexFromKey(String key) {
            return Integer.parseInt(key.split(Pattern.quote(":"))[1]);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Count dps per pair");
        job.setJarByClass(CountDpsPerPair.class);
        job.setMapperClass(CountDpsPerPair.MapperClass.class);
        job.setPartitionerClass(CountDpsPerPair.PartitionerClass.class);
        job.setReducerClass(CountDpsPerPair.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setCombinerClass(CountDpsPerPair.ReducerClass.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path joinedPairsAndLabels = new Path(args[0]);
        Path max_vec_size = new Path(args[1]);
        // multiple inputs
        MultipleInputs.addInputPath(job, joinedPairsAndLabels, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, max_vec_size, TextInputFormat.class, MapperClass.class);
        // single input
//        FileInputFormat.addInputPath(job, joinedPairsAndLabels);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
