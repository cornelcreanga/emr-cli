package org.ccreanga.awsutil.emr;

import org.ccreanga.awsutil.emr.model.EmrCluster;
import org.ccreanga.awsutil.emr.model.InstanceGroupType;
import org.slf4j.Logger;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.Instance;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.invoke.MethodHandles.lookup;
import static org.slf4j.LoggerFactory.getLogger;
import static software.amazon.awssdk.services.emr.model.InstanceState.RUNNING;

@CommandLine.Command(name = "emrcli",
        subcommands = {SshExecCommand.class, GangliaMetricsCommand.class})
public class ParentCommand implements Runnable {

    public static final Logger log = getLogger(lookup().lookupClass());

//    @CommandLine.Option(names = {"-user", "--user"}, required = false, description = "user")
//    protected String user;

//    @CommandLine.Option(names = {"-p", "--password"}, required = false, description = "password")
//    protected String password;
//
//    @CommandLine.Option(names = {"-k", "--keyPath"}, required = false, description = "private key location")
//    protected String keyLocation;

    @CommandLine.Option(names = {"-region", "--region"}, description = "aws region, default us-west-1")
    protected String region;

    @CommandLine.ArgGroup(multiplicity = "1")
    AwsAuth awsAuthGroup;

    @CommandLine.ArgGroup(multiplicity = "1")
    ClusterIdentification clusterGroup;

    @CommandLine.ArgGroup(multiplicity = "1")
    InstanceIdentification instanceIdGroup;

    String clusterId;

    protected String userName = System.getProperty("user.name");

    static class AwsAuth {
        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
        AwsKeys awsKeysGroup;
        @CommandLine.Option(names = {"-aws-profile", "--aws-profile"}, description = "aws profile name")
        protected String profile;
    }

    static class AwsKeys {
        @CommandLine.Option(names = {"-aws-access-key", "--aws-access-key"}, description = "aws access key")
        protected String accessKey;
        @CommandLine.Option(names = {"-aws-secret-accesskey", "--aws-secret-accesskey"}, description = "aws secret access key")
        protected String secretKey;
    }

    protected EmrClient emrClient;
    protected EmrCluster cluster;

    @Override
    public void run() {
        AwsCredentialsProvider credentialsProvider;
        if (awsAuthGroup.awsKeysGroup != null) {
            AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(awsAuthGroup.awsKeysGroup.accessKey, awsAuthGroup.awsKeysGroup.secretKey);
            credentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);
        } else {
            credentialsProvider = ProfileCredentialsProvider.create(awsAuthGroup.profile);
        }

        emrClient = EmrClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();

        clusterId = clusterGroup.clusterId;
        if (clusterId == null) {
            clusterId = EmrHelpers.getClusterId(emrClient, clusterGroup.clusterName);
        }

        cluster = EmrHelpers.getCluster(emrClient, clusterId);
    }

    public String getRegion() {
        return region;
    }

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new ParentCommand());
        commandLine.setExecutionStrategy(new CommandLine.RunAll());
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        if (args.length == 0) {
            commandLine.usage(System.out);
        } else
            commandLine.execute(args);
    }
}
