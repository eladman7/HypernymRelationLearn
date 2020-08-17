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
 * #4 MR app
 * Input: JoinPairLabel output and vector size
 * Map - write key as is
 * Reduce - write as is while ignores unlabeled pairs
 * arguments: 0- JoinPairLabel_out path, 1- vec_size path, 2-output path
 */
public class CountDpsPerPair {
    public static String VEC_SIZE_NAME = "vec_size";
    public static String FIRST_TAG = "*";

    public static Text removeTag(Text taggedKey) {
        return new Text(taggedKey.toString().replace(FIRST_TAG, "").trim());
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
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
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splittedGram = gram.toString().split("\\s+");
            // dog	animal	true
            String newVal, newKey;
            if (splittedGram.length == 3) {
                newKey = splittedGram[0] + "\t" + splittedGram[1] + "\t" + FIRST_TAG;
                newVal = splittedGram[2];
            }
            // dog	animal	0:X/NN/prep/2-like/VB/xcomp/1-Y/NN/acomp/0:animal	animal/NN/acomp/0 like/VB/xcomp/1 dog/NN/prep/2
            else {
                String[] dpSplit = splittedGram[2].split(":");
                newKey = splittedGram[0] + "\t" + splittedGram[1] + "\t" + dpSplit[0] + ":" + dpSplit[1];
                newVal = dpSplit[2];
            }
            context.write(new Text(newKey), new Text(newVal));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private String currentPair;
        private long lastIndexOfPair;
        private long counterOfIndexInPair;
        private long vec_size;

        public void setup(Context context) {
            currentPair = "";
            lastIndexOfPair = -1;
            counterOfIndexInPair = 0;
            vec_size = Integer.parseInt(context.getConfiguration().get(VEC_SIZE_NAME));
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
        * */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String key_str = removeTag(key).toString();
            String pair = extractPairFromKey(key_str);
            if (!pair.equals(currentPair)) {
                // fill gap from last index to vec size
                if (StringUtils.isNotEmpty(currentPair)) {
                    if (lastIndexOfPair + 1 < vec_size) {
                        for (long i = lastIndexOfPair + 1; i < vec_size; i++) {
                            context.write(new Text(currentPair), new Text(i + "," + "0"));
                        }
                    }
                }
                currentPair = pair;
                counterOfIndexInPair = 0;
                lastIndexOfPair = -1;
            }
            if (key.toString().contains(FIRST_TAG)) {
                Text next = values.iterator().next();
                context.write(new Text(key_str), next);
            } else {
                long index = extractDpIndexFromKey(key_str);
                // fill internal gaps of vector
                if (lastIndexOfPair + 1 < index) {
                    for (long i = lastIndexOfPair + 1; i < index; i++) {
                        context.write(new Text(currentPair), new Text(i + "," + "0"));
                    }
                }
                // continue with current index
                for (Text value : values) {
                    counterOfIndexInPair++;
                }
                context.write(new Text(currentPair), new Text(index + "," + String.valueOf(counterOfIndexInPair)));
                lastIndexOfPair = index;
                counterOfIndexInPair = 0;
            }
        }

        private String extractPairFromKey(String key) {
            String[] split = key.split("\\s+");
            return split[0] + "\t" + split[1];
        }

        private long extractDpIndexFromKey(String key) {
            String[] split = key.split("\\s+");
            return Long.parseLong(split[2].split(":")[0]);
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
        // setting max vector size from MR2 to all mappers׳ context in this stage
        List<String> strings = Files.readAllLines(Paths.get(args[1]));
        conf.set(VEC_SIZE_NAME, strings.get(0).split("\\s+")[1]);
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

        Path out3Input = new Path(args[0]);

        Path outputPath = new Path(args[2]);

        // SequenceFileInputFormat, TextInputFormat
        FileInputFormat.addInputPath(job, out3Input);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
