import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.SGD;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

import java.io.*;
import java.util.*;

public class Weka {
    // args:
    // 0- path to arff file

    public static void main(String[] args) throws Exception {
//         load data
        ArffLoader loader = new ArffLoader();
        loader.setFile(new File(args[0]));
        Instances instances = loader.getDataSet();
        instances.setClassIndex(instances.numAttributes() - 1);

        // cross validate a model
        Classifier classifier = new J48();
//        classifier.buildClassifier(instances);
        Evaluation eval = new Evaluation(instances);
        eval.crossValidateModel(classifier, instances, 10, new Random(1));
        System.out.println(eval.toSummaryString());
        System.out.println(eval.toClassDetailsString());
//        System.out.println(eval.);

        // build a map of:
        // pair -> [true_label, prediction]
        //todo:maybe needed here: classifier.buildClassifier(instances);
//        Map<String, Boolean[]> pairToLabels = new HashMap<>();
//        int index = 0;
//        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
//            String line;
//            String currentPair = null;
//            Boolean[] labels = new Boolean[2];
//            Boolean gotTrueLabel = false;
//            while ((line = br.readLine()) != null) {
//                if (gotTrueLabel) {
//                    labels = new Boolean[2];
//                    currentPair = null;
//                    gotTrueLabel = false;
//                }
//                if (line.startsWith("%")) {
//                    currentPair = line.split("\\s+")[1] + "\t" + line.split("\\s+")[2];
//                    double v = classifier.classifyInstance(instances.get(index));
//                    labels[1] = (v == 1);
//                    index++;
//                }
//                if ((line.toLowerCase().contains("true") || line.toLowerCase().contains("false")) && (!line.contains("class"))) {
//                    if (line.toLowerCase().contains("true")) {
//                        labels[0] = true;
//                    } else {
//                        labels[0] = false;
//                    }
//                    pairToLabels.put(currentPair, labels);
//                    gotTrueLabel = true;
//                }
//            }
//        }
//        // collect from model 5 pairs for each category: tn, fn, tp, fp
//        Boolean pred, label;
//        int fp_count = 0, tp_count = 0, fn_count = 0, tn_count = 0;
//        for (String pair : pairToLabels.keySet()) {
//            if (pair == null) continue;
//            Boolean[] labels = pairToLabels.get(pair);
//            pred = labels[1];
//            label = labels[0];
//            // alg say false when label is false - true negative
//            if (!pred && !label) {
//                if (tn_count < 5) {
//                    System.out.println("tn: " + pair);
//                    tn_count++;
//                }
//            }
//            // alg say false when label is true - false negative
//            else if (!pred && label) {
//                if (fn_count < 5) {
//                    System.out.println("fn: " + pair);
//                    fn_count++;
//                }
//            }
//            // alg say true when label is true - true positive
//            else if (pred && label) {
//                if (tp_count < 5) {
//                    System.out.println("tp: " + pair);
//                    tp_count++;
//                }
//            }
//            // alg say true when label is false - false positive
//            else if (pred && !label) {
//                if (fp_count < 5) {
//                    System.out.println("fp: " + pair);
//                    fp_count++;
//                }
//            }
//            if (fp_count == 5 && tp_count == 5 && fn_count == 5 && tn_count == 5)
//                break;
//        }
//        System.out.println("end");
    }
}
