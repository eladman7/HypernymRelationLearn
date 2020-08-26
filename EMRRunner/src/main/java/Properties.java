public class Properties {
    // consts
    public static String keyPair = "dsp_key";
    public static String DPMIN = "150";
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
    public static String Step1Arg1 = IN_BUCKET + "/threebiarcs";
    public static String Step1Arg2 = OUT_BUCKET + "/FilterAllDpsByDpmin_out_bar";
    public static String Step1Arg3 = DPMIN;

    // step2 - NumberDps
    public static String Step2Arg1 = OUT_BUCKET + "/FilterAllDpsByDpmin_out_bar/filteredDps";
    public static String Step2Arg2 = OUT_BUCKET + "/NumberDps_out_bar";

    // step3 - JoinDpIdsWithPairs
    public static String Step3Arg1 = OUT_BUCKET + "/FilterAllDpsByDpmin_out_bar/dpsToPair";
    public static String Step3Arg2 = OUT_BUCKET + "/NumberDps_out_bar/dpsToId";
    public static String Step3Arg3 = OUT_BUCKET + "/NumberDps_out_bar/vecSizes";
    public static String Step3Arg4 = OUT_BUCKET + "/JoinDpIdsWithPairs_out_bar";

//    // step4 - FindMaxVectorSize
//    public static String Step4Arg1 = OUT_BUCKET + "/NumberDps_out/vecSizes";
//    public static String Step4Arg2 = OUT_BUCKET + "/FindMaxVectorSize_out";

    // step5 - JoinPairLabel
    public static String Step5Arg1 = OUT_BUCKET + "/JoinDpIdsWithPairs_out_bar/pairToNumberedDps";
    public static String Step5Arg2 = IN_BUCKET + "/hypernym_pairs_annotated.txt";
    public static String Step5Arg3 = OUT_BUCKET + "/JoinDpIdsWithPairs_out_bar/maxVecSize";
    public static String Step5Arg4 = OUT_BUCKET + "/JoinPairLabel_out_bar";

    // step6 - CountDpsPerPair
    public static String Step6Arg1 = OUT_BUCKET + "/JoinPairLabel_out_bar";
    public static String Step6Arg2 = OUT_BUCKET + "/JoinDpIdsWithPairs_out_bar/maxVecSize";
    public static String Step6Arg3 = OUT_BUCKET + "/CountDpsPerPair_out_bar";

    // step7 - CreateLabeledVector
    public static String Step7Arg1 = OUT_BUCKET + "/CountDpsPerPair_out_bar";
    public static String Step7Arg2 = OUT_BUCKET + "/CreateLabeledVector_out_bar";

    // step8 - MergeVectors
    public static String Step8Arg1 = OUT_BUCKET + "/CreateLabeledVector_out_bar";
    public static String Step8Arg2 = OUT_BUCKET + "/JoinDpIdsWithPairs_out_bar/maxVecSize";
    public static String Step8Arg3 = OUT_BUCKET + "/MergeVectors_out_bar";
}