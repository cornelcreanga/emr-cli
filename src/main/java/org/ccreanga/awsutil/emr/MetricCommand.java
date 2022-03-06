package org.ccreanga.awsutil.emr;

import picocli.CommandLine;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;
import software.amazon.awssdk.services.emr.model.Instance;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(name = "metric")
public class MetricCommand implements Runnable {

    @CommandLine.ParentCommand
    private ParentCommand parent;

    @CommandLine.Option(names = {"-name", "--name"}, description = "Metric name")
    private String name;

    @CommandLine.Option(names = {"-stat", "--stat"}, description = "Statistic")
    private String statistic;

    @CommandLine.Option(names = {"-last", "--last"}, description = "Date in the past", required = true)
    private DateInThePastType last;

    @Override
    public void run() {

        CloudWatchClient cw = CloudWatchClient.builder()
                .region(Region.of(parent.region))
                .build();

        List<String> idList = new ArrayList<>();
        if (parent.instanceIdGroup.type != null) {
            idList = parent.cluster.filterEc2Ids(parent.instanceIdGroup.type);
        } else {
            String id = parent.instanceIdGroup.id;
            Instance instance = parent.cluster.instanceById(id).orElseThrow(() -> new RuntimeException("cant find ec2 machine " + id));
            idList.add(instance.id());
        }

        try {

            Instant startDate = DateInThePastType.pastInstant(last);
            Instant endDate = Instant.now();

            Dimension dimension = Dimension.builder()
                    .name("InstanceId")
                    .value(idList.get(0)).build();

            Metric met = Metric.builder()
                    .metricName(name)
                    .namespace("AWS/EC2")
                    .dimensions(dimension)
                    .build();

            MetricStat metStat = MetricStat.builder()
                    .stat(statistic)
                    .period(60)
                    .metric(met)
                    .build();

            MetricDataQuery dataQUery = MetricDataQuery.builder()
                    .metricStat(metStat)
                    .id("someid")
                    .returnData(true)
                    .build();

            List<MetricDataQuery> dq = new ArrayList<>();
            dq.add(dataQUery);

            GetMetricDataRequest getMetReq = GetMetricDataRequest.builder()
                    .maxDatapoints(100)
                    .startTime(startDate)
                    .endTime(endDate)
                    .metricDataQueries(dq)
                    .build();

            GetMetricDataResponse response = cw.getMetricData(getMetReq);
            List<MetricDataResult> data = response.metricDataResults();

            for (int i = 0; i < data.size(); i++) {
                MetricDataResult item = (MetricDataResult) data.get(i);

                System.out.println("The label is " + item.label());
                System.out.println("The status code is " + item.statusCode().toString());
            }

        } catch (CloudWatchException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
