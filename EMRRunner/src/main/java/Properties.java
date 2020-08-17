public class Properties {
    // consts
    public static String keyPair = "dsp_key";
    public static String DPMIN = "5";
    // todo:waiting for an answer in forum
    public static String BIARC_PATH = "???";

    // buckets
    public static String OUT_BUCKET = "s3n://ass3-output-bucket";
    public static String IN_BUCKET = "s3n://ass3-input-bucket";
    public static String LogsPath = "s3n://ass3-log-bucket";

    // input jars
    public static String firstJarPath = IN_BUCKET + "/MapReduce.jar";

    // step1 - FilterAllDpsByDpmin
    public static String Step1Arg1 = BIARC_PATH;
    public static String Step1Arg2 = OUT_BUCKET + "/FilterAllDpsByDpmin_out";
    public static String Step1Arg3 = DPMIN;

    // step2 - FindMaxVectorSize
    public static String Step2Arg1 = OUT_BUCKET + "/FilterAllDpsByDpmin_out/vecSizes";
    public static String Step2Arg2 = OUT_BUCKET + "/FindMaxVectorSize_out";

    // step3 - JoinPairLabel
    public static String Step3Arg1 = OUT_BUCKET + "//FilterAllDpsByDpmin_out/pairsToDps";
    public static String Step3Arg2 = IN_BUCKET + "/hypernym_pairs_annotated.txt";
    public static String Step3Arg3 = OUT_BUCKET + "/JoinPairLabel_out";

    // step4 - CountDpsPerPair
    public static String Step4Arg1 = OUT_BUCKET + "/JoinPairLabel_out";
    public static String Step4Arg2 = OUT_BUCKET + "/FindMaxVectorSize_out";
    public static String Step4Arg3 = OUT_BUCKET + "/CountDpsPerPair_out";

    // step5 - CreateLabeledVector
    public static String Step5Arg1 = OUT_BUCKET + "/CountDpsPerPair_out";
    public static String Step5Arg2 = OUT_BUCKET + "/CreateLabeledVector_out";
}