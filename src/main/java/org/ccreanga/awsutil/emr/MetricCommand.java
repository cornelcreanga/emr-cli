package org.ccreanga.awsutil.emr;

import picocli.CommandLine;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(name = "metric")
public class MetricCommand implements Runnable {

    @CommandLine.ParentCommand
    private ParentCommand parent;

    @CommandLine.Option(names = {"-metric", "--metric"}, description = "Metric")
    private String metric;

    @CommandLine.Option(names = {"-last", "--last"}, description = "Date in the past")
    private DateInThePastType last;

    @Override
    public void run() {

        CloudWatchClient cw = CloudWatchClient.builder()
                .region(Region.of(parent.region))
                .build();
        try {
            // Set the date
            Instant start = Instant.parse("2019-10-23T10:12:35Z");
            Instant endDate = Instant.now();

            Metric met = Metric.builder()
                    .metricName("DiskReadBytes")
                    .namespace("AWS/EC2")
                    .build();

            MetricStat metStat = MetricStat.builder()
                    .stat("Minimum")
                    .period(60)
                    .metric(met)
                    .build();

            MetricDataQuery dataQUery = MetricDataQuery.builder()
                    .metricStat(metStat)
                    .id("foo2")
                    .returnData(true)
                    .build();

            List<MetricDataQuery> dq = new ArrayList<>();
            dq.add(dataQUery);

            GetMetricDataRequest getMetReq = GetMetricDataRequest.builder()
                    .maxDatapoints(100)
                    .startTime(start)
                    .endTime(endDate)
                    .metricDataQueries(dq)
                    .build();

            GetMetricDataResponse response = cw.getMetricData(getMetReq);
            List<MetricDataResult> data = response.metricDataResults();

            for (int i = 0; i < data.size(); i++) {
                MetricDataResult item = (MetricDataResult) data.get(i);
                System.out.println("The label is "+item.label());
                System.out.println("The status code is "+item.statusCode().toString());
            }

        } catch (CloudWatchException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
