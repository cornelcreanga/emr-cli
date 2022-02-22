package org.ccreanga.awsutil.emr;

// snippet-start:[emr.java2.describe_cluster.import]
import org.ccreanga.awsutil.emr.model.EmrCluster;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
// snippet-end:[emr.java2.describe_cluster.import]


/*
 *   Ensure that you have setup your development environment, including your credentials.
 *   For information, see this documentation topic:
 *
 *   https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 *
 */
public class DescribeCluster {

    public static void main(String[] args){



        //String clusterId = "beta-p-emr-darklaunch02" ;
        String clusterId = "j-3EP433NL21P5A" ;
        Region region = Region.US_WEST_1;
        EmrClient emrClient = EmrClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create("csa"))
                .region(region)
                .build();

        EmrCluster cluster = EmrHelpers.getCluster(emrClient, clusterId);
//        describeMyCluster(emrClient, clusterId);
//
//        ListInstancesRequest listInstancesRequest = ListInstancesRequest.builder().clusterId(clusterId).build();
//        ListInstancesResponse response = emrClient.listInstances(listInstancesRequest);
//        List<Instance> list = response.instances();
//
//        List<InstanceGroup> listInstanceGroups =
//                emrClient.listInstanceGroups(ListInstanceGroupsRequest.builder().clusterId(clusterId).build()).instanceGroups();

        emrClient.close();
    }

    // snippet-start:[emr.java2.describe_cluster.main]
    public static void describeMyCluster(EmrClient emrClient, String clusterId){
        EmrHelpers.getClusterId(emrClient,"beta-p-emr-darklaunch02");
        try {
            DescribeClusterRequest clusterRequest = DescribeClusterRequest.builder()
                    .clusterId(clusterId)
                    .build();

            DescribeClusterResponse response = emrClient.describeCluster(clusterRequest);
            System.out.println("The name of the cluster is "+response.cluster().name());

        } catch(EmrException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
    // snippet-end:[emr.java2.describe_cluster.main]
}
