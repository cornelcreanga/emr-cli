package org.ccreanga.awsutil.emr;

import org.apache.sshd.client.SshClient;
import org.ccreanga.awsutil.emr.model.EmrCluster;
import org.ccreanga.awsutil.emr.ssh.SshCommandRunner;
import org.ccreanga.awsutil.emr.ssh.SshConnection;
import org.ccreanga.awsutil.emr.ssh.SshResponse;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.stream.Collectors.groupingBy;
import static software.amazon.awssdk.services.emr.model.ClusterState.RUNNING;
import static software.amazon.awssdk.services.emr.model.ClusterState.WAITING;

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

        } catch (InvalidRequestException e) {
            throw new RuntimeException(e.awsErrorDetails().errorMessage());
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
            ListClustersRequest request = ListClustersRequest.builder().clusterStates(RUNNING, WAITING).build();
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

    public static void runCommands(String userName, List<String> commands, List<String> ipList, Map<String, String> out, Map<String, String> err, int maxConnections) {
        ExecutorService executorService = new ThreadPoolExecutor(Math.min(5, maxConnections), maxConnections, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        try {

            Map<String, Future<List<SshResponse>>> futures = new HashMap<>();
            SshClient client = SshClient.setUpDefaultClient();
            client.start();

            for (String ip : ipList) {

                SshConnection sshConnection = new SshConnection(userName, ip);

                Callable<List<SshResponse>> commandRunner = new SshCommandRunner(client, sshConnection, commands, 30L);
                Future<List<SshResponse>> future = executorService.submit(commandRunner);
                futures.put(ip, future);
            }

            futures.forEach((ip, future) -> {
                try {
                    List<SshResponse> response = future.get();
                    StringBuilder sbOut = new StringBuilder(1024);
                    StringBuilder sbErr = new StringBuilder(1024);
                    for (SshResponse sshResponse : response) {
                        if (sshResponse.getStdOutput().length() > 0) {
                            sbOut.append(sshResponse.getStdOutput()).append("\n");
                        }
                        if (sshResponse.getErrOutput().length() > 0) {
                            sbErr.append(sshResponse.getErrOutput()).append("\n");
                        }
                    }
                    if (sbOut.length() > 0) {
                        out.put(ip, sbOut.toString());
                    }
                    if (sbErr.length() > 0) {
                        err.put(ip, sbOut.toString());
                    }
                } catch (Exception e) {
                    e.printStackTrace();//todo
                }
            });


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        executorService.shutdown();
    }


}
