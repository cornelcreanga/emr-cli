package org.ccreanga.awsutil.emr;

import picocli.CommandLine;

import java.util.*;

@CommandLine.Command(name = "ganglia")
public class GangliaMetricsCommand implements Runnable {

    @CommandLine.ParentCommand
    private ParentCommand parent;

    @CommandLine.ArgGroup(multiplicity = "1")
    private ClusterIdentification clusterGroup;

    @CommandLine.ArgGroup(multiplicity = "1")
    InstanceIdentification instanceIdGroup;

    @CommandLine.Option(names = {"-host", "--host"}, description = "Host")
    private String host;

    @CommandLine.Option(names = {"-metric", "--metric"}, description = "Metric")
    private String metric;

    @CommandLine.Option(names = {"-last", "--last"}, description = "Date in the past")
    private DateInThePastType last;

    @Override
    public void run() {
//http://10.26.49.39/ganglia/graph.php?r=day&c=j-3TMSDEOTVHOD0&h=ip-10-26-48-27.us-west-1.compute.internal&m=cpu_userjson=1
        List<String> commands = Collections.singletonList("");
        Map<String, String> out = new HashMap<>();
        Map<String, String> err = new HashMap<>();

        try {

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
