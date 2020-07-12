public class LingusticUtils {

    public static boolean isNoun(String wordDescription){
        return wordDescription.contains("NN") ||
                wordDescription.contains("NNS") ||
                wordDescription.contains("NNP") ||
                wordDescription.contains("NNPS");
        //todo: bar learn all the possible nouns in all world languages
    }
}
