import org.apache.hadoop.io.Text;

import java.util.Map;

public class NGramData {
    private Text nGram;
    private Map<String, String> graph;
    private Map<String, String> wordToWordDesc;

    public Text getnGram() {
        return nGram;
    }

    public void setnGram(Text nGram) {
        this.nGram = nGram;
    }

    public Map<String, String> getGraph() {
        return graph;
    }

    public void setGraph(Map<String, String> graph) {
        this.graph = graph;
    }

    public Map<String, String> getWordToWordDesc() {
        return wordToWordDesc;
    }

    public void setWordToWordDesc(Map<String, String> wordToWordDesc) {
        this.wordToWordDesc = wordToWordDesc;
    }
}
