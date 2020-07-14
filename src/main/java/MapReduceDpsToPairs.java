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

        // TODO: 13/07/2020 add stemmer.
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            String[] splitNGram = gram.toString().trim().split("\\s+");
            int KEY_FIRST = 0; // FIRST KEY PART INDEX IN STRING
            int KEY_SECOND = 1;// SEC KEY PART INDEX IN STRING
            String key = splitNGram[KEY_FIRST] + " " + splitNGram[KEY_SECOND];
            String dp = extractDps(key, extractNgram(splitNGram));
            context.write(new Text(dp), new Text(key));
        }

        private String extractNgram(String[] splitNGram) {
            StringBuilder ngram = new StringBuilder();
            int GRAM_INDEX = 3; // NGRAM INDEX IN STRING
            for (int i = GRAM_INDEX; i< splitNGram.length; i++) {
                ngram.append(splitNGram[i]);
                ngram.append('\t');
            }
            return ngram.toString();
        }


        private String extractDps(String key, String ngram) {
            String[] splitNGram = ngram.trim().split("\\s+");
            HashMap<String, String> graph = new HashMap<>();
            buildGraph(graph, splitNGram);  // Build the graph from the ngram
            return generalizeGraphString(key,  build_string_from_graph(key, graph, splitNGram));
        }

        private StringBuilder build_string_from_graph(String key, HashMap<String, String> graph, String[]splitNGram) {
            String key_first = key.split("\\s+")[0]; // From
            String key_sec = key.split("\\s+")[1];  // To
            String next = graph.get(key_first);

            StringBuilder dp = new StringBuilder(getFullGram(splitNGram, key_first) + " ");
            while (!isLastNode(key_sec, next)) {
                dp.append(next).append(" ");
                next = graph.get(getWord(next)); // Move to the next node.
            }
            dp.append(next);
            return dp;
        }


        private boolean isLastNode(String key_sec, String next) {
            return next.split("/")[0].equals(key_sec);
        }

        private String generalizeGraphString(String key, StringBuilder dp) {
            String key_first = key.split("\\s+")[0]; // From
            String key_sec = key.split("\\s+")[1];  // To
            StringBuilder dp_res = new StringBuilder();
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

        private void buildGraph(HashMap<String, String> graph, String[] splitNGram) {
            for(int index = 0; index < splitNGram.length; index++) {
                String word = splitNGram[index];   // Take only the noun
                graph.put(getWord(word), splitNGram[Integer.parseInt(get_next(word))]);
                if(index != 0) {
                    graph.put(word.split("/")[0], splitNGram[Integer.parseInt(get_next(word)) - 1]);
                }
            }
        }

        private String get_next(String gram) {
            String[] splitNGram = gram.trim().split("/");
            return splitNGram[splitNGram.length - 1];

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


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for( Text pair : values) {
                context.write(key, pair);
            }
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
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