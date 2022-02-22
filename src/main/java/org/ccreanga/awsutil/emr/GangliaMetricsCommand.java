package org.ccreanga.awsutil.emr;

import picocli.CommandLine;

@CommandLine.Command(name = "ganglia")
public class GangliaMetricsCommand implements Runnable {

    private enum DateInThePastType {
        HOUR1, HOUR2, HOUR4, DAY, WEEK, MONTH, YEAR
    }

    @CommandLine.ParentCommand
    private ParentCommand parent;

    @CommandLine.ArgGroup(multiplicity = "1")
    private ClusterIdentification clusterGroup;

    @CommandLine.Option(names = {"-host", "--host"}, description = "Host")
    private String host;

    @CommandLine.Option(names = {"-metric", "--metric"}, description = "Metric")
    private String metric;

    @CommandLine.Option(names = {"-last", "--last"}, description = "Date in the past")
    private DateInThePastType last;

    @Override
    public void run() {

    }
}
