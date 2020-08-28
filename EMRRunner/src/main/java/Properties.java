public class Properties {
    // consts
    public static String keyPair = "dsp_key";
    public static String DPMIN = "750";
    public static Integer NUM_OF_INSTANCES = 9;
    public static String S3_PREFIX = "s3n://";

    // buckets
    public static String OUT_BUCKET = S3_PREFIX + "ass3-output-bucket";
    public static String IN_BUCKET = S3_PREFIX + "ass3-input-bucket";
    public static String LogsPath = S3_PREFIX + "ass3-log-bucket";

    // input jars
    public static String firstJarPath = IN_BUCKET + "/MapReduce.jar";
    // step1 - FilterAllDpsByDpmin
    // sample file name   /biarcs.05-of-99
    public static String Step1Arg1 = IN_BUCKET + "/biarcs";
    public static String Step1Arg2 = OUT_BUCKET + "/FilterAllDpsByDpmin_out";
    public static String Step1Arg3 = DPMIN;

    // step2 - NumberDps
    public static String Step2Arg1 = OUT_BUCKET + "/FilterAllDpsByDpmin_out/filteredDps";
    public static String Step2Arg2 = OUT_BUCKET + "/NumberDps_out";

    // step3 - JoinDpIdsWithPairs
    public static String Step3Arg1 = OUT_BUCKET + "/FilterAllDpsByDpmin_out/dpsToPair";
    public static String Step3Arg2 = OUT_BUCKET + "/NumberDps_out/dpsToId";
    public static String Step3Arg3 = OUT_BUCKET + "/NumberDps_out/vecSizes";
    public static String Step3Arg4 = OUT_BUCKET + "/JoinDpIdsWithPairs_out";

//    // step4 - FindMaxVectorSize
//    public static String Step4Arg1 = OUT_BUCKET + "/NumberDps_out/vecSizes";
//    public static String Step4Arg2 = OUT_BUCKET + "/FindMaxVectorSize_out";

    // step5 - JoinPairLabel
    public static String Step5Arg1 = OUT_BUCKET + "/JoinDpIdsWithPairs_out/pairToNumberedDps";
    public static String Step5Arg2 = IN_BUCKET + "/hypernym_pairs_annotated.txt";
    public static String Step5Arg3 = OUT_BUCKET + "/JoinDpIdsWithPairs_out/maxVecSize";
    public static String Step5Arg4 = OUT_BUCKET + "/JoinPairLabel_out";

    // step6 - CountDpsPerPair
    public static String Step6Arg1 = OUT_BUCKET + "/JoinPairLabel_out";
    public static String Step6Arg2 = OUT_BUCKET + "/JoinDpIdsWithPairs_out/maxVecSize";
    public static String Step6Arg3 = OUT_BUCKET + "/CountDpsPerPair_out";

    // step7 - CreateLabeledVector
    public static String Step7Arg1 = OUT_BUCKET + "/CountDpsPerPair_out";
    public static String Step7Arg2 = OUT_BUCKET + "/CreateLabeledVector_out";

    // step8 - MergeVectors
    public static String Step8Arg1 = OUT_BUCKET + "/CreateLabeledVector_out";
    public static String Step8Arg2 = OUT_BUCKET + "/JoinDpIdsWithPairs_out/maxVecSize";
    public static String Step8Arg3 = OUT_BUCKET + "/MergeVectors_out";
}