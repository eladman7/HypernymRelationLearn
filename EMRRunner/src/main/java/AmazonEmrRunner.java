import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AmazonEmrRunner {

    public static void main(String[] args) {

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().
                withRegion(Regions.US_EAST_1).build();
        List<StepConfig> stepConfigs = new ArrayList<>();

        // basic cluster step
        StepConfig enableDebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new StepFactory().newEnableDebuggingStep());
        stepConfigs.add(enableDebugging);

        // STEP1
        String[] args1 = {Properties.Step1Arg1, Properties.Step1Arg2, Properties.Step1Arg3};
        stepConfigs.add(buildStep(args1, "FilterAllDpsByDpmin", "Filter DPs above DPMin"));

        // STEP2
        String[] args2 = {Properties.Step2Arg1, Properties.Step2Arg2};
        stepConfigs.add(buildStep(args2, "NumberDps", "Number dps"));

        // STEP3
        String[] args3 = {Properties.Step3Arg1, Properties.Step3Arg2, Properties.Step3Arg3, Properties.Step3Arg4};
        stepConfigs.add(buildStep(args3, "JoinDpIdsWithPairs", "Join dp ids and pairs"));


        // STEP5
        String[] args5 = {Properties.Step5Arg1, Properties.Step5Arg2, Properties.Step5Arg3, Properties.Step5Arg4};
        stepConfigs.add(buildStep(args5, "JoinPairLabel", "Join pairs and labels"));

        // STEP6
        String[] args6 = {Properties.Step6Arg1, Properties.Step6Arg2, Properties.Step6Arg3};
        stepConfigs.add(buildStep(args6, "CountDpsPerPair", "Count dps per pair"));

        // STEP7
        String[] args7 = {Properties.Step7Arg1, Properties.Step7Arg2};
        stepConfigs.add(buildStep(args7, "CreateLabeledVector", "Create labeled examples"));

        // STEP8
        String[] args8 = {Properties.Step8Arg1, Properties.Step8Arg2, Properties.Step8Arg3};
        stepConfigs.add(buildStep(args8, "MergeVectors", "Merge labeled examples"));

        Map<String, String> jvmProperties = new HashMap<>();
        jvmProperties.put("mapred.child.java.opts", "-Xmx4096m");

        Configuration myConfig = new Configuration()
                .withClassification("mapred-site")
                .withProperties(jvmProperties);

        // run
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()      // collection of steps
                .withName("Full Run Big Data")                                   //cluster name
                .withInstances(defineInstances())
                .withSteps(stepConfigs)
                .withLogUri(Properties.LogsPath)
                .withConfigurations(myConfig);

        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        runFlowRequest.withReleaseLabel("emr-5.3.0");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static JobFlowInstancesConfig defineInstances() {
        return new JobFlowInstancesConfig()
                .withInstanceCount(Properties.NUM_OF_INSTANCES)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.7.3").withEc2KeyName(Properties.keyPair)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
    }

    private static StepConfig buildStep(String[] args, String mainClass, String stepName) {
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar(Properties.firstJarPath) // This should be a full map reduce application.
                .withMainClass(mainClass)
                .withArgs(args); // A list of command line args passed to the jar files main function when executed

        return new StepConfig()
                .withName(stepName)
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }
}
