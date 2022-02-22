package org.ccreanga.awsutil.emr;

import org.ccreanga.awsutil.emr.model.EmrCluster;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.*;

import static java.util.stream.Collectors.groupingBy;

public class EmrHelpers {

    public static EmrCluster getCluster(EmrClient emrClient, String clusterId) {
        try {
            DescribeClusterRequest clusterRequest = DescribeClusterRequest.builder()
                    .clusterId(clusterId)
                    .build();

            DescribeClusterResponse response = emrClient.describeCluster(clusterRequest);
            Cluster cluster = response.cluster();
            List<Instance> instances = getAllClusterInstances(emrClient, clusterId);
            List<InstanceGroup> listInstanceGroups = getAllInstanceGroups(emrClient, clusterId);

            Map<String, List<Instance>> instanceMap = instances.stream().collect(groupingBy(Instance::instanceGroupId));

            Map<InstanceGroup, List<Instance>> instanceGroupMap = new HashMap<>();
            for (InstanceGroup instanceGroup : listInstanceGroups) {
                List<Instance> list = instanceMap.get(instanceGroup.id());
                if (list == null)
                    list = Collections.emptyList();
                instanceGroupMap.put(instanceGroup, list);
            }
            return new EmrCluster(cluster, instanceGroupMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Instance> getAllClusterInstances(EmrClient emrClient, String clusterId) {
        List<Instance> list = new ArrayList<>();
        try {
            ListInstancesRequest request = ListInstancesRequest.builder().clusterId(clusterId).build();
            while (true) {
                ListInstancesResponse response = emrClient.listInstances(request);
                List<Instance> instances = response.instances();
                list.addAll(instances);
                if (response.marker() == null) {
                    break;
                }
                final String marker = response.marker();
                request = request.copy(builder1 -> builder1.marker(marker));
            }
        } catch (EmrException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    public static List<InstanceGroup> getAllInstanceGroups(EmrClient emrClient, String clusterId) {
        List<InstanceGroup> list = new ArrayList<>();
        try {
            ListInstanceGroupsRequest request = ListInstanceGroupsRequest.builder().clusterId(clusterId).build();
            while (true) {

                ListInstanceGroupsResponse response = emrClient.listInstanceGroups(request);
                List<InstanceGroup> instances = response.instanceGroups();
                list.addAll(instances);
                if (response.marker() == null) {
                    break;
                }
                final String marker = response.marker();
                request = request.copy(builder1 -> builder1.marker(marker));
            }
        } catch (EmrException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    public static String getClusterId(EmrClient emrClient, String clusterName) {
        try {
            ListClustersRequest request = ListClustersRequest.builder().clusterStates(ClusterState.RUNNING).build();
            while (true) {
                ListClustersResponse response = emrClient.listClusters(request);
                List<ClusterSummary> clusters = response.clusters();
                for (ClusterSummary cluster : clusters) {
                    if (cluster.name().equals(clusterName))
                        return cluster.id();
                }
                if (response.marker() == null) {
                    break;
                }
                final String marker = response.marker();
                request = request.copy(builder1 -> builder1.marker(marker));
            }
        } catch (EmrException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
