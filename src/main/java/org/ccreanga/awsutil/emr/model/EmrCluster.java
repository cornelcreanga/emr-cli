package org.ccreanga.awsutil.emr.model;

import software.amazon.awssdk.services.emr.model.Cluster;
import software.amazon.awssdk.services.emr.model.Instance;
import software.amazon.awssdk.services.emr.model.InstanceGroup;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static software.amazon.awssdk.services.emr.model.InstanceState.RUNNING;

public class EmrCluster {

    private final Cluster cluster;
    private final Map<InstanceGroup, List<Instance>> instanceGroups;

    public EmrCluster(Cluster cluster, Map<InstanceGroup, List<Instance>> instanceGroups) {
        this.cluster = cluster;
        this.instanceGroups = new HashMap<>(instanceGroups);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Map<InstanceGroup, List<Instance>> getInstanceGroups() {
        return Collections.unmodifiableMap(instanceGroups);
    }

    public Instance getMaster() {
        return filterInstances(InstanceGroupType.MASTER).get(0);
    }

    public List<String> filterIp(InstanceGroupType type){
        List<String> ipList = new ArrayList<>();
        instanceGroups.forEach((instanceGroup, instances) -> {
            if ((InstanceGroupType.ALL == type) ||
                    (instanceGroup.instanceGroupType().name().equals(type.name()))) {

                List<String> ips = instances.stream().
                        filter(instance -> instance.status().state() == RUNNING).
                        map(Instance::privateIpAddress).
                        collect(Collectors.toList());
                ipList.addAll(ips);
            }
        });
        return ipList;
    }

    public List<String> filterEc2Ids(InstanceGroupType type){
        List<String> ipList = new ArrayList<>();
        instanceGroups.forEach((instanceGroup, instances) -> {
            if ((InstanceGroupType.ALL == type) ||
                    (instanceGroup.instanceGroupType().name().equals(type.name()))) {

                List<String> ips = instances.stream().
                        filter(instance -> instance.status().state() == RUNNING).
                        map(Instance::ec2InstanceId).
                        collect(Collectors.toList());
                ipList.addAll(ips);
            }
        });
        return ipList;
    }

    public List<Instance> filterInstances(InstanceGroupType type){
        List<Instance> ipList = new ArrayList<>();
        instanceGroups.forEach((instanceGroup, instances) -> {
            if ((InstanceGroupType.ALL == type) ||
                    (instanceGroup.instanceGroupType().name().equals(type.name()))) {

                List<Instance> ips = instances.stream().filter(instance -> instance.status().state() == RUNNING).collect(Collectors.toList());
                ipList.addAll(ips);
            }
        });
        return ipList;
    }

    public Optional<Instance> instanceById(String id){

        Set<InstanceGroup> groups = instanceGroups.keySet();
        for (InstanceGroup next : groups) {
            List<Instance> instances = instanceGroups.get(next);
            List<Instance> found = instances.stream().filter(instance -> instance.id().equals(id)).collect(Collectors.toList());
            if (!found.isEmpty())
                return Optional.of(found.get(0));

        }

        return Optional.empty();
    }

}
