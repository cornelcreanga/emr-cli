package org.ccreanga.awsutil.emr;

import picocli.CommandLine;
import software.amazon.awssdk.services.emr.model.Instance;

import java.util.*;

@CommandLine.Command(name = "ganglia")
public class GangliaMetricsCommand implements Runnable {

    @CommandLine.ParentCommand
    private ParentCommand parent;

    @CommandLine.Option(names = {"-host", "--host"}, description = "Host")
    private String host;

    @CommandLine.Option(names = {"-name", "--name"}, description = "Metric name")
    private String name;

    @CommandLine.Option(names = {"-last", "--last"}, description = "Date in the past")
    private DateInThePastType last;

    private String format = "wget -O - \"http://%s/ganglia/graph.php?r=%s&c=%s&h=%s&m=%s&json=1\"";

    @Override
    public void run() {
//http://10.26.49.39/ganglia/graph.php?r=day&c=j-3TMSDEOTVHOD0&h=ip-10-26-48-27.us-west-1.compute.internal&m=cpu_userjson=1
        //List<String> commands = Collections.singletonList("");
        Map<String, String> out = new HashMap<>();
        Map<String, String> err = new HashMap<>();

        List<Instance> instanceList = new ArrayList<>();
        if (parent.instanceIdGroup.type != null) {
            instanceList.addAll(parent.cluster.filterInstances(parent.instanceIdGroup.type));
        } else {
            String id = parent.instanceIdGroup.id;
            Instance instance = parent.cluster.instanceById(id).orElseThrow(() -> new RuntimeException("cant find ec2 machine " + id));
            instanceList.add(instance);
        }

        List<String> commands = new ArrayList<>();
        String masterIp = parent.cluster.getMaster().privateIpAddress();
        for (Iterator<Instance> iterator = instanceList.iterator(); iterator.hasNext(); ) {
            Instance instance = iterator.next();
            String command = String.format(format, masterIp, toGangliaName(last), parent.cluster.getCluster().id(), instance.privateDnsName(), name);
            commands.add(command);
        }

        EmrHelpers.runCommands(parent.userName, commands, Collections.singletonList(masterIp), out, err);

        System.out.println("done");
    }


    private String toGangliaName(DateInThePastType type) {
        switch (type) {
            case HOUR1:
                return "hour";
            case HOUR2:
                return "2h";
            case HOUR4:
                return "4h";
            case DAY:
                return "day";
            case WEEK:
                return "week";
            case MONTH:
                return "month";
            case YEAR:
                return "year";
        }
        throw new IllegalArgumentException("unhandled time option");
    }
}
