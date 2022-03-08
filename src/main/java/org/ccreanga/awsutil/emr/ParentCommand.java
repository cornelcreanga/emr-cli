package org.ccreanga.awsutil.emr;

import org.ccreanga.awsutil.emr.model.EmrCluster;
import picocli.CommandLine;
import picocli.CommandLine.*;
import org.slf4j.Logger;
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
        subcommands = {SshExecCommand.class, GangliaMetricsCommand.class, MetricCommand.class})
public class ParentCommand implements Runnable {

    public static final Logger log = getLogger(lookup().lookupClass());

//    @CommandLine.Option(names = {"-user", "--user"}, required = false, description = "user")
//    protected String user;

//    @CommandLine.Option(names = {"-p", "--password"}, required = false, description = "password")
//    protected String password;
//
//    @CommandLine.Option(names = {"-k", "--keyPath"}, required = false, description = "private key location")
//    protected String keyLocation;

    protected int maxSshConnection = 5;

    @Option(names = {"-region", "--region"}, description = "aws region, default us-west-1")
    protected String region;

    @ArgGroup(multiplicity = "1")
    AwsAuth awsAuthGroup;

    @ArgGroup(multiplicity = "1")
    ClusterIdentification clusterGroup;

    @ArgGroup(multiplicity = "1")
    InstanceIdentification instanceIdGroup;

    @Spec
    Model.CommandSpec spec;

    String clusterId;

    protected String userName = System.getProperty("user.name");

    static class AwsAuth {
        @ArgGroup(exclusive = false, multiplicity = "1")
        AwsKeys awsKeysGroup;
        @Option(names = {"-aws-profile", "--aws-profile"}, description = "aws profile name")
        protected String profile;
    }

    static class AwsKeys {
        @Option(names = {"-aws-access-key", "--aws-access-key"}, description = "aws access key")
        protected String accessKey;
        @Option(names = {"-aws-secret-accesskey", "--aws-secret-accesskey"}, description = "aws secret access key")
        protected String secretKey;
    }

    protected EmrClient emrClient;
    protected EmrCluster cluster;

    @Option(names = {"-maxSsh", "--maxSsh"}, description = "Maximum no of concurrent SSH connections")
    public void setMaxSshConnection(int maxSshConnection) {
        if (maxSshConnection <= 0 || maxSshConnection>100) {
            throw new ParameterException(spec.commandLine(), "Maximum no of concurrent SSH connections should be between 1 and 100");
        }
        this.maxSshConnection = maxSshConnection;
    }

    @Override
    public void run() {
        try {
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
        }catch (Exception e){
            System.out.println(e.getMessage());
            System.exit(-1);
        }
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
