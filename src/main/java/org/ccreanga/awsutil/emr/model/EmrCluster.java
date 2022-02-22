package org.ccreanga.awsutil.emr.model;

import software.amazon.awssdk.services.emr.model.Cluster;
import software.amazon.awssdk.services.emr.model.Instance;
import software.amazon.awssdk.services.emr.model.InstanceGroup;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmrCluster {

    private Cluster cluster;
    private Map<InstanceGroup, List<Instance>> instanceGroups;

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

}
