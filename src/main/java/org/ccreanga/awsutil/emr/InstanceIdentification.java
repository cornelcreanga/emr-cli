package org.ccreanga.awsutil.emr;

import org.ccreanga.awsutil.emr.model.InstanceGroupType;
import picocli.CommandLine;

class InstanceIdentification {
    @CommandLine.Option(names = {"-type", "--type"}, description = "Type")
    InstanceGroupType type;

    @CommandLine.Option(names = {"-id", "--id"}, description = "Ec2 Id")
    String id;
}
