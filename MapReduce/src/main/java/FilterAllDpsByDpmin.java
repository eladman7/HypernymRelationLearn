import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * #1 MR app
 * Input: Google׳s syntactic Biarcs data set
 * Map - create all dependency paths between any 2 nouns (nouns are stemmed)
 * Reduce - outputs pair to dp+ngram but only for dps which occurs at least DPMIN times in different pairs
 * arguments: 0- input path, 1-output path, 2-DPMIN
 */
public class FilterAllDpsByDpmin {
    public static String DPMIN_NAME = "DPMIN";
    public static String COUNTER_TAG = "!";
    public static String VALUES_TAG = "*";

    public static Text removeTag(Text taggedKey) {
        return new Text(taggedKey.toString().replace(COUNTER_TAG, "").replace(VALUES_TAG, "").trim());
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return partitionForValue(key, numPartitions);
        }

        public static int partitionForValue(Text value, int numPartitions) {
            return (removeTag(value).hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private long[] counters;
        int numOfReducers;

        public void setup(Context context) {
            numOfReducers = context.getNumReduceTasks();
            System.out.println("num of reducers: " + numOfReducers);
            counters = new long[numOfReducers];
        }

        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            Text stemmedGram = stemNouns(gram);
            // for each pair we get its both variations <a,b>, <b,a>
            List<String> allNounPairsByNgram = createAllNounPairsByNgram(stemmedGram);
            // for each pair look for a dependency path in its biarc
            Map<String, String> pairToDp = buildDpsFromPairs(allNounPairsByNgram, stemmedGram);
            // example for writing from design doc: <<x like y>:dog-animal , [ngram1]>
            // or  <<x like y> , [dog-animal:ngram1]>
            Text taggedKey;
            for (String pair : pairToDp.keySet()) {
                taggedKey = new Text(VALUES_TAG + pairToDp.get(pair));
                context.write(taggedKey, new Text(pair + ":" + stemmedGram));
                counters[PartitionerClass.partitionForValue(taggedKey, numOfReducers)]++;
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int c = 0; c < counters.length - 1; c++) {
                if (counters[c] > 0) {
                    context.write(new Text(COUNTER_TAG + (c + 1)), new Text(String.valueOf(counters[c])));
                }
                counters[c + 1] += counters[c];
            }
        }

        private Text stemNouns(Text gram) {
//            animal	animal/NN/acomp/0 like/VB/xcomp/1 dog/NN/prep/2
            String[] gramSplit = gram.toString().split("\\s+");
            StringBuilder sb = new StringBuilder();
            String stemmedWord, word;
            boolean stemRoot = false;
            for (int i = 1; i < gramSplit.length; i++) {
                if (LingusticUtils.isNoun(gramSplit[i])) {
                    word = gramSplit[i].split("/")[0];
                    if (word.equals(gramSplit[0])) stemRoot = true;
                    stemmedWord = LingusticUtils.stem(word);
                    gramSplit[i] = gramSplit[i].replace(word, stemmedWord);
                }
                sb.append(gramSplit[i]);
                sb.append(" ");
            }
            sb.deleteCharAt(sb.length() - 1);
            if (stemRoot) {
                return new Text(LingusticUtils.stem(gramSplit[0]) + "\t" + sb.toString());
            }
            return new Text(gramSplit[0] + "\t" + sb.toString());
        }

        // todo: if we found path for pair <a,b> but not for <b,a>
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
        private long currentDPValuesCounter;
        private String currentKey;
        private MultipleOutputs<Text, Text> mo;
        private int DPMIN;
        private long initialOffset;
        private long uniqueDPId;

        public void setup(Context context) {
            mo = new MultipleOutputs<>(context);
            DPMIN = Integer.parseInt(context.getConfiguration().get(DPMIN_NAME));
            currentDPValuesCounter = 0;
            initialOffset = 0;
            currentKey = "";
            uniqueDPId = -1;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("reducer class id: " + System.identityHashCode(this));
            if (key.toString().contains(COUNTER_TAG)) {
                long offsetsSum = 0;
                for (Text value : values) {
                    offsetsSum += Long.parseLong(value.toString());
                }
                initialOffset = offsetsSum;
                return;
            }
            // count values till reaching DPMin
            List<String> tempList = new ArrayList<>(DPMIN);
            for (Text pairWithNgram : values) {
                currentDPValuesCounter++;
                tempList.add(pairWithNgram.toString());
                if (currentDPValuesCounter == DPMIN) break;
            }
            if (currentDPValuesCounter < DPMIN) return;
            // key changed
            String newKey = removeTag(key);
            if (!newKey.equals(currentKey)) {
                uniqueDPId++;
            }
            // example from doc: Write(<dog, animal> ,[ 0:<x like y>:ngram1]>)
            // write temp list
            String dpId = String.valueOf(initialOffset + uniqueDPId);
            for (int i = 0; i < currentDPValuesCounter; i++) {
                String[] splitPairWithNgram = tempList.get(i).split(":");
                mo.write(new Text(splitPairWithNgram[0]), new Text(dpId + ":" + newKey + ":"
                                + splitPairWithNgram[1]),
                        "pairsToDps/pairsToDp");
            }
            if (!newKey.equals(currentKey)) {
                currentDPValuesCounter = 0;
            }
            // write the rest of the values
            for (Text pairWithNgram : values) {
                String[] splitPairWithNgram = pairWithNgram.toString().split(":");
                mo.write(new Text(splitPairWithNgram[0]),
                        new Text(dpId + ":" + newKey + ":" + splitPairWithNgram[1]),
                        "pairsToDps/pairsToDp");
            }
        }

        private String removeTag(Text key) {
            return key.toString().replace(VALUES_TAG, "");
        }


        public void cleanup(Context context) throws IOException, InterruptedException {
            mo.write(new Text("vec_size"), new Text(String.valueOf(uniqueDPId + 1)), "vecSizes/vecSize");
            mo.close();
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
        //TextInputFormat
        job.setInputFormatClass(SequenceFileInputFormat.class);
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
