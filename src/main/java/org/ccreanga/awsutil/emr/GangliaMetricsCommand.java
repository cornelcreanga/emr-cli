package org.ccreanga.awsutil.emr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.ccreanga.awsutil.emr.ganglia.GangliaMetric;
import org.ccreanga.awsutil.emr.ganglia.GangliaReport;
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
    private GangliaMetric metricName;

    @CommandLine.Option(names = {"-last", "--last"}, description = "Date in the past")
    private DateInThePastType last;

    private static final String format = "wget -O - \"http://%s/ganglia/graph.php?r=%s&c=%s&h=%s&m=%s&json=1\"";

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void run() {

        try {
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
            for (Instance instance : instanceList) {
                String command = String.format(format, masterIp, toGangliaName(last), parent.cluster.getCluster().id(), instance.privateDnsName(), metricName.name().toLowerCase());
                commands.add(command);
            }

            EmrHelpers.runCommands(parent.userName, commands, Collections.singletonList(masterIp), out, err);

            Set<String> keys = out.keySet();

            if (keys.isEmpty()) {
                System.out.println("no data was found");
            } else {
                String key = (String) keys.toArray()[0];
                String[] json = out.get(key).split("\n");
                for (String s : json) {
                    GangliaReport report = mapper.readValue(s, GangliaReport[].class)[0];
                    System.out.printf("%s\t%.2f\t%.2f \n", report.getMetric_name(), report.avg(), report.max());
                }
            }

        }catch (Exception e){
           throw new RuntimeException(e);
        }
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
