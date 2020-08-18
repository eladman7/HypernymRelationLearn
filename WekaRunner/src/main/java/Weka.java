import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

import java.io.File;
import java.util.Random;

public class Weka {
    // args:
    // 0- path to combined r- files
    // 1- number of folds
    public static void main(String[] args) throws Exception {
        CSVLoader loader = new CSVLoader();
        loader.setSource(new File(args[0]));
        loader.setNoHeaderRowPresent(true);

        Instances instances = loader.getDataSet();
        instances.setClassIndex(instances.numAttributes() - 1);

        Classifier m = new NaiveBayes();
        m.buildClassifier(instances);

        Evaluation eval = new Evaluation(instances);
        eval.crossValidateModel(m, instances, Integer.parseInt(args[1]), new Random(1));

        System.out.println(eval.toSummaryString());
    }
}
