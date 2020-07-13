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
import java.util.HashMap;

public class MapReduceDpsToPairs {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) {

        }

        // TODO: 13/07/2020 add stemmer.
        @Override

        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splittedNGram = gram.toString().trim().split("\\s+");
            StringBuilder ngram = new StringBuilder("");
            String key = splittedNGram[0] + " " + splittedNGram[1];
            for (int i = 3; i< splittedNGram.length; i++) {
                ngram.append(splittedNGram[i]);
                ngram.append('\t');
            }

            String dp = extractDps(key, ngram.toString());
            context.write(new Text(dp), new Text(key));

        }


        private String extractDps(String key, String ngram) {
            HashMap<String, String> graph = new HashMap<>();
            String[] splittedNGram = ngram.trim().split("\\s+");

            for(int index = 0; index < splittedNGram.length; index++) {
                String word = splittedNGram[index];   // Take only the noun
                graph.put(getWord(word), splittedNGram[Integer.valueOf(get_next(word))]);
                if(index != 0) {
                    graph.put(word.split("/")[0], splittedNGram[Integer.valueOf(get_next(word)) - 1]);
                }
            }

            String key_first = key.split("\\s+")[0]; // From
            String key_sec = key.split("\\s+")[1];  // To
            String next = graph.get(key_first);
            StringBuilder dp = new StringBuilder(getFullGram(splittedNGram, key_first) + " ");
            StringBuilder dp_res = new StringBuilder();
            // TODO: 13/07/2020 What if there are no path?
            while (!next.split("/")[0].equals(key_sec)) {
                dp.append(next).append(" ");
                next = graph.get(getWord(next)); // Move to the next node.
            }
            dp.append(next);
            System.out.println(dp.toString());
            String[] splitted = dp.toString().split("\\s+");
            for(String str : splitted) {
                /* TODO: 13/07/2020 should be X?!? */
                if(LingusticUtils.isNoun(str) && (str.contains(key_first) || str.contains(key_sec))) {
                    dp_res.append(str.replace(str.split("/")[0], "X")).append(" ");
                } else {
                    dp_res.append(str).append(" ");
                }
            }
            return dp_res.toString();
        }

        private String get_next(String gram) {
            String[] splittedNGram = gram.trim().split("/");
            return splittedNGram[splittedNGram.length - 1];

        }

        private String getWord(String gram) {
            return gram.split("/")[0];
        }

        private String getFullGram(String [] grams, String gram){
            for(String word : grams) {
                if (word.contains(gram)) {
                    return word;
                }
            }
            return "";
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
            System.out.println("in reducer");
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
        Job job = new Job(conf, "Make dps to pairs table");
        job.setJarByClass(MapReduceDpsToPairs.class);
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
        Path input_path = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}