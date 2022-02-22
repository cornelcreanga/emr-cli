package org.ccreanga.awsutil.emr;

import picocli.CommandLine;

class ClusterIdentification {
    @CommandLine.Option(names = {"-cluster-name", "--cluster-name"}, description = "Cluster name")
    String clusterName;

    @CommandLine.Option(names = {"-cluster-id", "--cluster-id"}, description = "Cluster id")
    String clusterId;
}
