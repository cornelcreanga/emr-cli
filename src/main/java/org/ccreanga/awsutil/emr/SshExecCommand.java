package org.ccreanga.awsutil.emr;

import org.apache.sshd.client.SshClient;
import org.ccreanga.awsutil.emr.model.EmrCluster;
import org.ccreanga.awsutil.emr.model.InstanceGroupType;
import org.ccreanga.awsutil.emr.ssh.SshCommandRunner;
import org.ccreanga.awsutil.emr.ssh.SshConnection;
import org.ccreanga.awsutil.emr.ssh.SshResponse;
import picocli.CommandLine;
import software.amazon.awssdk.services.emr.model.Instance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.awssdk.services.emr.model.InstanceState.RUNNING;

@CommandLine.Command(name = "exec")
public class SshExecCommand implements Runnable {

    @CommandLine.ParentCommand
    private ParentCommand parent;

    @CommandLine.ArgGroup(multiplicity = "1")
    Command commandGroup;

    static class Command {
        @CommandLine.Option(names = {"-command", "--command"}, description = "Command")
        private String command;

        @CommandLine.Option(names = {"-file", "--file"}, description = "File containing commands (one per line)")
        private String file;
    }

    public void run() {
        List<String> commands = buildCommandList();
        Map<String, String> out = new HashMap<>();
        Map<String, String> err = new HashMap<>();

        List<String> ipList = new ArrayList<>();
        if (parent.instanceIdGroup.type != null) {
            ipList = parent.cluster.filterIp(parent.instanceIdGroup.type);
        } else {
            String id = parent.instanceIdGroup.id;
            Instance instance = parent.cluster.instanceById(id).orElseThrow(() -> new RuntimeException("cant find ec2 machine " + id));
            ipList.add(instance.privateIpAddress());
        }
        EmrHelpers.runCommands(parent.userName, commands, ipList, out, err, parent.maxSshConnection);

        out.forEach((key, value) -> {
            try {
                Files.write(Paths.get(key.replace('.', '-') + ".out"), value.getBytes(UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        err.forEach((key, value) -> {
            try {
                Files.write(Paths.get(key.replace('.', '-') + ".out"), value.getBytes(UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }


    private List<String> buildCommandList() {
        if (commandGroup.command != null)
            return Collections.singletonList(commandGroup.command);
        File file = new File(commandGroup.file);
        if (!file.exists()) {
            System.err.println("can't find file " + commandGroup.command);
            System.exit(1);
        }
        try {
            return Files.readAllLines(file.toPath(), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
