package org.ccreanga.awsutil.emr.ganglia;

import java.util.List;

public class GangliaReport {

    private String ds_name;
    private String cluster_name;
    private String graph_type;
    private String host_name;
    private String metric_name;
    private String color;
    private List<List<String>> datapoints;

    public GangliaReport() {
    }

    public GangliaReport(String ds_name, String cluster_name, String graph_type, String host_name, String metric_name, String color, List<List<String>> datapoints) {
        this.ds_name = ds_name;
        this.cluster_name = cluster_name;
        this.graph_type = graph_type;
        this.host_name = host_name;
        this.metric_name = metric_name;
        this.color = color;
        this.datapoints = datapoints;
    }

    public String getDs_name() {
        return ds_name;
    }

    public void setDs_name(String ds_name) {
        this.ds_name = ds_name;
    }

    public String getCluster_name() {
        return cluster_name;
    }

    public void setCluster_name(String cluster_name) {
        this.cluster_name = cluster_name;
    }

    public String getGraph_type() {
        return graph_type;
    }

    public void setGraph_type(String graph_type) {
        this.graph_type = graph_type;
    }

    public String getHost_name() {
        return host_name;
    }

    public void setHost_name(String host_name) {
        this.host_name = host_name;
    }

    public String getMetric_name() {
        return metric_name;
    }

    public void setMetric_name(String metric_name) {
        this.metric_name = metric_name;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public List<List<String>> getDatapoints() {
        return datapoints;
    }

    public void setDatapoints(List<List<String>> datapoints) {
        this.datapoints = datapoints;
    }

    public double avg(){
        double total = 0;
        int items = 0;
        for (List<String> next : datapoints) {
            String value = next.get(0);
            if (value.equals("NaN"))
                continue;
            long timestamp = Long.parseLong(next.get(1));
            items++;
            total += Double.parseDouble(value);
        }
        return total/items;
    }
    public double max(){
        double max = 0;
        int items = 0;
        for (List<String> next : datapoints) {
            String value = next.get(0);
            if (value.equals("NaN"))
                continue;
            long timestamp = Long.parseLong(next.get(1));
            items++;
            double parsed = Double.parseDouble(value);
            if (parsed>max)
                max = parsed;
        }
        return max;
    }


}
