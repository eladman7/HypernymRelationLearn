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
import java.util.ArrayList;
import java.util.List;

public class FilterHypernymBiarcsMR {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) {

        }

        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splittedNGram = gram.toString().trim().split("\\s+");
            if (splittedNGram.length == 3) {// annotated pair
                if (Boolean.parseBoolean(splittedNGram[2])) {
                    String key = splittedNGram[0] + "\t" + splittedNGram[1];
                    context.write(new Text(key + "1"), new Text("true"));
                }
            } else {
                List<String> allNounPairsByNgram = createAllNounPairsByNgram(gram);
                for (String pair : allNounPairsByNgram) {
                    context.write(new Text(pair + "2"), gram);
                }
            }
        }

        private List<String> createAllNounPairsByNgram(Text gram) {
            List<String> result = new ArrayList<>();
            String[] splitNGram = gram.toString().trim().split("\\s+");
            List<String> onlyNounsLst = new ArrayList<>();
            for (int i = 0; i < splitNGram.length - 1; i++) {
                if (LingusticUtils.isNoun(splitNGram[i + 1])) {
                    onlyNounsLst.add(splitNGram[i + 1]);
                }
            }
            int half = (onlyNounsLst.size() % 2 == 0) ? onlyNounsLst.size() / 2 : (onlyNounsLst.size() / 2) + 1;
            for (int i = 0; i < half; i++) {
                for (int j = i + 1; j < onlyNounsLst.size(); j++) {
                    result.add(extractWord(onlyNounsLst.get(i)) + "\t" + extractWord(onlyNounsLst.get(j)));
                    result.add(extractWord(onlyNounsLst.get(j)) + "\t" + extractWord(onlyNounsLst.get(i)));
                }
            }
            return result;
        }

        private String extractWord(String wordDescription) {
            return wordDescription.substring(0, wordDescription.indexOf("/"));
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public String currentKey;

        public void setup(Context context) {
            currentKey = "";
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text firstVal = values.iterator().next();
            String extractTag = extractTag(key);
            if (firstVal.toString().contains("true") && getTag(key).equals("1")) {
                currentKey = extractTag;
            } else {
                if (extractTag.equals(currentKey)) {
                    context.write(new Text(extractTag), firstVal);
                    for (Text value : values) {
                        context.write(new Text(extractTag), value);
                    }
                }
            }
        }


        public void cleanup(Context context) {
        }
    }

    public static String getTag(Text key) {
        return String.valueOf(key.toString().charAt(key.toString().length() - 1));
    }

    public static String extractTag(Text key) {
        return key.toString().substring(0, key.toString().length() - 1);
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (extractTag(key).hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "filter only biarcs with hypernym");
        job.setJarByClass(FilterHypernymBiarcsMR.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//      job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path oneGram = new Path(args[0]);
        Path twoGram = new Path(args[1]);

//      Path decs = new Path(args[2]);
        Path outputPath = new Path(args[2]);
        MultipleInputs.addInputPath(job, oneGram, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, twoGram, TextInputFormat.class, MapperClass.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
