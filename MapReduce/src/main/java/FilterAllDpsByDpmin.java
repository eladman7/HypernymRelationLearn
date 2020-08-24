import org.apache.commons.collections.CollectionUtils;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * #1 MR app
 * Input: Google׳s syntactic Biarcs data set
 * Map - create all dependency paths between any 2 nouns (nouns are stemmed)
 * Reduce - outputs pair to dp+ngram but only for dps which occurs at least DPMIN times in different pairs
 * arguments: 0- input path, 1-output path, 2-DPMIN
 */
public class FilterAllDpsByDpmin {
    public static String DPMIN_NAME = "DPMIN";
    public static String VALUES_TAG = "*";

    public static String removeTag(Text taggedKey) {
        String res = taggedKey.toString();
        if (res.startsWith(VALUES_TAG)) {
            res = res.replaceFirst(Pattern.quote(VALUES_TAG), ""); // remove VEC_SIZE_TAG
        }
        return res.trim();
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return partitionForValue(key, numPartitions);
        }

        public static int partitionForValue(Text key, int numPartitions) {
            return (removeTag(key).hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text gram, Context context) throws IOException, InterruptedException {
            gram = removeBiarcTail(gram);
            if (!isValidNgram(gram)) return;
            Text stemmedGram = stemNouns(gram);
            // for each pair we get its both variations <a,b>, <b,a>
            List<String> allNounPairsByNgram = createAllNounPairsByNgram(stemmedGram);
            // for each pair look for a dependency path in its biarc
            Map<String, String> pairToDp = buildDpsFromPairs(allNounPairsByNgram, stemmedGram);
            // example for writing from design doc: <<x like y>:dog-animal , [ngram1]>
            // or  <<x like y> , [dog-animal:ngram1]>
            Text taggedKey;
            String extractedPair;
            String[] pairSplit;
            for (String pair : pairToDp.keySet()) {
                taggedKey = new Text(VALUES_TAG + pairToDp.get(pair));
                pairSplit = pair.split("\\s+");
                extractedPair = extractWord(pairSplit[0]) + "\t" + extractWord(pairSplit[1]);
                context.write(taggedKey, new Text(extractedPair + ":" + stemmedGram.toString()));
            }
        }

        // filtering cases like this:
        // a missing word
        // are	are/VBP/ROOT/0 branch/NN/nn/3 //NN/attr/1	10	1968,3	1974,2	1981,4	1993,1
        // todo: maybe if noun appears more than once in an ngram fail it
        private boolean isValidNgram(Text gram) {
            String[] splittedGram = gram.toString().split("\\s+");
            if (StringUtils.isEmpty(splittedGram[0])) return false;
            for (int i = 1; i < splittedGram.length; i++) {
                if (!validWordDesc(splittedGram[i])) return false;
                if (!validWord(splittedGram[i])) return false;
            }
            return true;
        }

        // filter special chars within words: !@#$%^&*()_+-=,./;:'"[{}]\|
        private boolean validWord(String wordDesc) {
            String specialChars = "!@#$%^&*()_+-=,./;:'[{}]\\|;\"";
            String word = wordDesc.split(Pattern.quote("/"))[0];
            for (int i = 0; i < specialChars.length(); i++) {
                if (word.contains(String.valueOf(specialChars.charAt(i)))) return false;
            }
            return true;
        }

        // filtering cases like this: //NN/attr/1
        private boolean validWordDesc(String wordDesc) {
            return wordDesc.split(Pattern.quote("/")).length == 4;
        }

        private Text stemNouns(Text gram) {
//            animal	animal/NN/acomp/0 like/VB/xcomp/1 dog/NN/prep/2
            String[] gramSplit = gram.toString().split("\\s+");
            StringBuilder sb = new StringBuilder();
            String stemmedWord, word;
            boolean stemRoot = false;
            for (int i = 1; i < gramSplit.length; i++) {
                if (LingusticUtils.isNoun(gramSplit[i])) {
                    word = gramSplit[i].split(Pattern.quote("/"))[0];
                    if (word.equals(gramSplit[0]) && extractPointer(gramSplit[i]) == 0) stemRoot = true;
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

        //abounded	abounded/VBD/ccomp/0 in/IN/prep/1 land/JJ/dep/4 mongers/NNS/pobj/2	22	1927,7	1930,9	1945,1	1946,1	1954,3	2005,1
        private Text removeBiarcTail(Text gram) {
            String[] splitBiarc = gram.toString().split("\\s+");
            StringBuilder validBiarcPart = new StringBuilder(splitBiarc[0]);
            validBiarcPart.append("\t");
            for (int i = 1; i < splitBiarc.length; i++) {
                if (splitBiarc[i].split(Pattern.quote("/")).length > 1) {
                    validBiarcPart.append(splitBiarc[i]);
                    validBiarcPart.append(" ");
                }
            }
            return new Text(validBiarcPart.toString().trim());
        }

        private Map<String, String> buildDpsFromPairs(List<String> ngramNounPairs, Text gram) {
            if (CollectionUtils.isEmpty(ngramNounPairs)) return new HashMap<>();
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
                String[] wordSplit = gramSplit[i].split(Pattern.quote("/"));
                int pointer = Integer.parseInt(wordSplit[3]);
                if (pointer != 0) {
                    graph.put(gramSplit[i], gramSplit[pointer]);
                } else {
                    graph.put(gramSplit[i], getWordDescOfFirstWord(gramSplit[pointer], gram.toString()));
                }
            }
            return graph;
        }

        private String getWordDescOfFirstWord(String s, String gram) {
            String[] gramSplit = gram.split("\\s+");
            for (int i = 1; i < gramSplit.length; i++) {
                if (extractWord(gramSplit[i]).equals(s) && extractPointer(gramSplit[i]) == 0) return gramSplit[i];
            }
            return null;
        }

        private String getWordDescByWord(String s, String gram) {
            String[] gramSplit = gram.split("\\s+");
            for (int i = 1; i < gramSplit.length; i++) {
                if (extractWord(gramSplit[i]).equals(s)) return gramSplit[i];
            }
            return null;
        }

        /**
         * searches for dp from a to b when given a pair <a, b>
         * note: not searching for <b, a> since this pair will appear later on
         *
         * @param nGramData
         * @param pair
         * @return dp if found or null otherwise
         */
        private String buildDp(NGramData nGramData, String pair) {
            String[] splitPair = pair.split("\\s+");
            if (splitPair[0].equals(nGramData.getGraph().get(splitPair[0])))
                return null;

            StringBuilder dp = new StringBuilder();
            String currentKey = splitPair[0];
            String target = splitPair[1];
            dp.append(currentKey.replaceFirst(Pattern.quote(extractWord(currentKey)), "X"));
//            dp.append(currentKey);
            String currentVal;
            int counter = 0;
            do {
                if (counter == nGramData.getnGram().toString().split("\\s+").length) {
                    System.out.println("***************************************");
                    System.out.println("BAD NGRAM!!!! " + nGramData.getnGram().toString());
                    System.out.println("counter: " + counter + " pair: " + pair);
                    System.out.println("***************************************");
//                    abound	abound/NN/attr/0 in/IN/prep/1 countri/NNS/pobj/2 in/IN/prep/3 world/NN/pobj/4
                }
                dp.append("-");
                currentVal = nGramData.getGraph().get(currentKey);
                // if found self arc its a dead end
                if (currentKey.equals(currentVal)) {
                    return null;
                }
                // found a loop
                else if (currentVal.equals(splitPair[0])) {
                    return null;
                } else if (currentVal.equals(target)) {
                    dp.append(currentVal.replaceFirst(Pattern.quote(extractWord(target)), "Y"));
//                    dp.append(currentVal);
                } else dp.append(currentVal);
                counter++;
                currentKey = currentVal;
            } while (!currentVal.equals(target));
//            return cleanDP(dp.toString());
            return dp.toString();
        }

        // input: X/NN/advmod/2-as/JJ/prep/1-Y/NN/acomp/0
        // output: X/advmod-as/prep-Y/acomp
        private String cleanDP(String rawDp) {
            StringBuilder cleanDp = new StringBuilder();
            String[] nodes = rawDp.split(Pattern.quote("-"));
            for (String node : nodes) {
                String[] splittedNode = node.split(Pattern.quote("/"));
                cleanDp.append(splittedNode[0]).append("/").append(splittedNode[2]).append("-");
            }
            // remove last '-'
            cleanDp.deleteCharAt(cleanDp.length() - 1);
            return cleanDp.toString();
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
//                    result.add(extractWord(onlyNounsLst.get(i)) + "\t" + extractWord(onlyNounsLst.get(j)));
                    result.add(onlyNounsLst.get(i) + "\t" + onlyNounsLst.get(j));
                }
            }
            return result;
        }

        private int extractPointer(String wordDescription) {
            return Integer.parseInt(wordDescription.substring(wordDescription.lastIndexOf("/") + 1));
        }

        private String extractWord(String wordDescription) {
            return wordDescription.substring(0, wordDescription.indexOf("/"));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mo;
        private long currentDPValuesCounter;
        private int DPMIN;

        public void setup(Context context) {
            DPMIN = Integer.parseInt(context.getConfiguration().get(DPMIN_NAME));
            currentDPValuesCounter = 0;
            mo = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // count values till reaching DPMin
            List<String> tempList = new ArrayList<>(DPMIN);
            for (Text pairWithNgram : values) {
                currentDPValuesCounter++;
                tempList.add(pairWithNgram.toString());
                if (currentDPValuesCounter == DPMIN) break;
            }
            if (currentDPValuesCounter < DPMIN) {
                currentDPValuesCounter = 0;
                return;
            }
            // key changed
            String newKey = removeTag(key);
            currentDPValuesCounter = 0;
            mo.write(new Text(newKey), null, "filteredDps/filteredDp");
            for (int i = 0; i < DPMIN; i++) {
                mo.write(new Text(newKey), new Text(tempList.get(i)), "dpsToPair/dpToPair");
            }
            // write the rest of the values
            for (Text value : values) {
                mo.write(new Text(newKey), value, "dpsToPair/dpToPair");
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
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
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path biarcsInput = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, biarcsInput);

        // multiple outputs
        MultipleOutputs.addNamedOutput(job, "dpsToPair", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "filteredDps", TextOutputFormat.class,
                Text.class, Text.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
