import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Weka {
    // args:
    // 0- number of folds
    public static void main(String[] args) throws Exception {
        S3ObjectInputStream inputStream = getExamplesFile();

        CSVLoader loader = new CSVLoader();
        loader.setSource(inputStream);
        loader.setNoHeaderRowPresent(true);

        Instances instances = loader.getDataSet();
        instances.setClassIndex(instances.numAttributes() - 1);

        Classifier m = new NaiveBayes();
        m.buildClassifier(instances);

        Evaluation eval = new Evaluation(instances);
        eval.crossValidateModel(m, instances, Integer.parseInt(args[0]), new Random(1));

        System.out.println(eval.toSummaryString());
    }

    private static S3ObjectInputStream getExamplesFile() throws Exception {
        String bucketName = "ass3-output-bucket";
        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest()
                        .withBucketName(bucketName)
                        .withPrefix("MergeVectors_out" + "/");
        List<String> keys = new ArrayList<>();
        AmazonS3 s3Client = AmazonS3Client.builder().withRegion(Regions.US_EAST_1).build();
        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        // retrieves up to 1k of file summaries. no need for batching
        List<S3ObjectSummary> summaries = objects.getObjectSummaries();
        if (summaries.size() < 1) {
            throw new Exception("Error: no files in cluster output folder");
        }
        summaries.stream().filter(summary -> summary.getSize() > 0)
                .forEach(s -> keys.add(s.getKey()));
        if (keys.size() != 1) {
            throw new Exception("Error: number of relevant files is different from 1");
        }
        S3Object csvFile = s3Client.getObject(new GetObjectRequest(bucketName, keys.get(0)));
        return csvFile.getObjectContent();
    }
}
