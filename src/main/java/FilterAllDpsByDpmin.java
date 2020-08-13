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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * #1 MR app
 * Input: Google׳s syntactic Biarcs data set
 * Map - create all dependency paths between any 2 nouns
 * Reduce - outputs pair to dp+ngram but only for dps which occurs at least DPMIN times in different pairs
 * arguments: 0- input path, 1-output path, 2-DPMIN
 */
public class FilterAllDpsByDpmin {
    public static String DPMIN_NAME = "DPMIN";
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        // TODO: 13/07/2020 add stemmer.
        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            // for each pair we get its both variations <a,b>, <b,a>
            List<String> allNounPairsByNgram = createAllNounPairsByNgram(gram);
            // for each pair look for a dependency path in its biarc
            Map<String, String> pairToDp = buildDpsFromPairs(allNounPairsByNgram, gram);
            // example for writing from design doc: <<x like y>:dog-animal , [ngram1]>
            // or  <<x like y> , [dog-animal:ngram1]>
            for (String pair : pairToDp.keySet()) {
                context.write(new Text(pairToDp.get(pair)), new Text(pair + ":" + gram));
            }
        }

        // todo:elad if we found path for pair <a,b> but not for <b,a>
        //  should we associate both pairs with this dp ?
        private Map<String, String> buildDpsFromPairs(List<String> ngramNounPairs, Text gram) {
            NGramData nGramData = new NGramData();
            nGramData.setGraph(buildNgramGraph(gram));
            nGramData.setWordToWordDesc(buildWordToDescMap(gram));
            nGramData.setnGram(gram);
            Map<String, String> pairToDp = new HashMap<>();
            String dp;
            for (String pair : ngramNounPairs) {
                dp = buildDp(nGramData, pair);
                if (dp != null) {
                    pairToDp.put(pair, dp);
                }
                String flippedPair = flipPair(pair);
                dp = buildDp(nGramData, flippedPair);
                if (dp != null) {
                    pairToDp.put(flippedPair, dp);
                }
            }
            return pairToDp;
        }

        private String flipPair(String pair) {
            String[] pairSplit = pair.split("\\s+");
            return pairSplit[1] + "\t" + pairSplit[0];
        }

        private Map<String, String> buildWordToDescMap(Text gram) {
            Map<String, String> wordToDesc = new HashMap<>();
            String[] gramSplit = gram.toString().split("\\s+");
            for (int i = 1; i < gramSplit.length; i++) {
                wordToDesc.put(extractWord(gramSplit[i]), gramSplit[i]);
            }
            return wordToDesc;
        }

        private Map<String, String> buildNgramGraph(Text gram) {
            Map<String, String> graph = new HashMap<>();
            String[] gramSplit = gram.toString().split("\\s+");
            // chair	furniture/pobj/acomp/0 like/VB/xcomp/1 tahat/NN/prep/2
            for (int i = 1; i < gramSplit.length; i++) {
                String[] wordSplit = gramSplit[i].split("/");
                int pointer = Integer.parseInt(wordSplit[3]);
                graph.put(wordSplit[0], gramSplit[pointer]);
            }
            return graph;
        }

        /**
         * searches for dp from a to b when given a pair <a, b>
         * note: not searching for <b, a> since this pair will appear later on
         *
         * @param nGramData
         * @param pair
         * @return dp if found or null otherwise
         */
        // todo: what should we exclude from the dp?
        private String buildDp(NGramData nGramData, String pair) {
            String[] splitPair = pair.split("\\s+");
            if (splitPair[0].equals(nGramData.getGraph().get(splitPair[0]))) return null;

            StringBuilder dp = new StringBuilder();
            String currentKey = splitPair[0];
            String target = splitPair[1];
            dp.append(nGramData.getWordToWordDesc().get(currentKey).replace(currentKey, "X"));
            String currentVal;
            do {
                dp.append("-");
                currentVal = nGramData.getGraph().get(currentKey);
                // if found self arc its a dead end
                if (currentKey.equals(currentVal)) {
                    return null;
                } else if (extractWord(currentVal).equals(target)) {
                    dp.append(currentVal.replace(target, "Y"));
                } else dp.append(currentVal);
                currentKey = extractWord(currentVal);
            } while (!(extractWord(currentVal).equals(target)));
            return dp.toString();
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
                }
            }
            return result;
        }

        private String extractWord(String wordDescription) {
            return wordDescription.substring(0, wordDescription.indexOf("/"));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private long currentCounter;
        private long vectorLengthCounter;
        private MultipleOutputs<Text, Text> mo;
        private int DPMIN;

        public void setup(Context context) {
            currentCounter = 0;
            vectorLengthCounter = 0;
            mo = new MultipleOutputs<>(context);
            DPMIN = Integer.parseInt(context.getConfiguration().get(DPMIN_NAME));
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> tempList = new ArrayList<>(DPMIN);
            for (Text pairWithNgram : values) {
                currentCounter++;
                tempList.add(pairWithNgram.toString());
                if (currentCounter == DPMIN) break;
            }
            if (currentCounter < DPMIN) return;
            // exmaple from doc: Write(<dog, animal> ,[ 0:<x like y>:ngram1]>)
            // write temp list
            for (int i = 0; i < currentCounter; i++) {
                String[] splitPairWithNgram = tempList.get(i).split(":");
                mo.write(new Text(splitPairWithNgram[0]), new Text(i + ":" + key.toString() + ":" + splitPairWithNgram[1]),
                        "pairsToDps/pairsToDp");
            }
            // write the rest of the values
            for (Text pairWithNgram : values) {
                String[] splitPairWithNgram = pairWithNgram.toString().split(":");
                mo.write(new Text(splitPairWithNgram[0]),
                        new Text(currentCounter + ":" + key.toString() + ":" + splitPairWithNgram[1]),
                        "pairsToDps/pairsToDp");
                currentCounter++;
            }
            if (vectorLengthCounter < currentCounter) vectorLengthCounter = currentCounter;
            currentCounter = 0;
        }


        public void cleanup(Context context) throws IOException, InterruptedException {
            mo.write(new Text("vec_size"), new Text(String.valueOf(vectorLengthCounter)), "vecSizes/vecSize");
            mo.close();
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
        conf.set(DPMIN_NAME, args[2]);
        Job job = new Job(conf, "Filter all dps by DPMin");
        job.setJarByClass(FilterAllDpsByDpmin.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path biarcsInput = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, biarcsInput);
        MultipleOutputs.addNamedOutput(job, "pairsToDps", TextOutputFormat.class,
                Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "vecSizes", TextOutputFormat.class,
                Text.class, LongWritable.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
