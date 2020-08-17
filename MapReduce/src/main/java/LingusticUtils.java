public class LingusticUtils {

    public static boolean isNoun(String wordDescription) {
        return wordDescription.contains("NN") ||
                wordDescription.contains("NNS") ||
                wordDescription.contains("NNP") ||
                wordDescription.contains("NNPS");
    }

    public static String stem(String word) {
        char[] wordAsArr = word.toCharArray();
        Stemmer stemmer = new Stemmer();
        stemmer.add(wordAsArr, wordAsArr.length);
        stemmer.stem();
        return stemmer.toString();
    }
}
