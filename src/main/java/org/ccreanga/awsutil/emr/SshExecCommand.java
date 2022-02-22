package org.ccreanga.awsutil.emr;

import org.apache.sshd.client.SshClient;
import org.ccreanga.awsutil.emr.model.EmrCluster;
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

    private enum InstanceGroupType {
        ALL, MASTER, CORE, TASK
    }

    @CommandLine.ParentCommand
    private ParentCommand parent;

    @CommandLine.ArgGroup(multiplicity = "1")
    ClusterIdentification clusterGroup;

    @CommandLine.ArgGroup(multiplicity = "1")
    InstanceIdentification instanceIdGroup;

    @CommandLine.ArgGroup(multiplicity = "1")
    Command commandGroup;

    static class InstanceIdentification {
        @CommandLine.Option(names = {"-type", "--type"}, description = "Type")
        private InstanceGroupType type;

        @CommandLine.Option(names = {"-host", "--host"}, description = "Host")
        private String host;
    }

    static class Command {
        @CommandLine.Option(names = {"-command", "--command"}, description = "Command")
        private String command;

        @CommandLine.Option(names = {"-command-file", "--command-file"}, description = "File containing commands (one per line)")
        private String file;
    }

    public void run() {
        String userName = System.getProperty("user.name");
        ExecutorService executorService = new ThreadPoolExecutor(8, 32, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        try {
            String clusterId = clusterGroup.clusterId;
            if (clusterId == null) {
                clusterId = EmrHelpers.getClusterId(parent.emrClient, clusterGroup.clusterName);
            }

            List<String> ipList = new ArrayList<>();
            if (instanceIdGroup.type != null) {

                EmrCluster cluster = EmrHelpers.getCluster(parent.emrClient, clusterId);

                cluster.getInstanceGroups().forEach((instanceGroup, instances) -> {
                    if ((InstanceGroupType.ALL == instanceIdGroup.type) ||
                            (instanceGroup.instanceGroupType().name().equals(instanceIdGroup.type.name()))) {

                        List<String> ips = instances.stream().
                                filter(instance -> instance.status().state() == RUNNING).
                                map(Instance::privateIpAddress).
                                collect(Collectors.toList());
                        ipList.addAll(ips);
                    }
                });
            } else {
                ipList.add(instanceIdGroup.host);
            }

            Map<String, Future<List<SshResponse>>> futures = new HashMap<>();
            SshClient client = SshClient.setUpDefaultClient();
            client.start();

            for (String ip : ipList) {

                SshConnection sshConnection = new SshConnection(userName, ip);
                List<String> commands = buildCommandList();

                Callable<List<SshResponse>> commandRunner = new SshCommandRunner(client, sshConnection, commands, 30L);
                Future<List<SshResponse>> future = executorService.submit(commandRunner);
                futures.put(ip, future);
            }

            futures.forEach((ip, future) -> {
                try {
                    List<SshResponse> response = future.get();
                    StringBuilder sbOut = new StringBuilder(1024);
                    StringBuilder sbErr = new StringBuilder(1024);
                    for (SshResponse sshResponse : response) {
                        if (sshResponse.getStdOutput().length() > 0) {
                            sbOut.append(sshResponse.getStdOutput()).append("\n");
                        }
                        if (sshResponse.getErrOutput().length() > 0) {
                            sbErr.append(sshResponse.getErrOutput()).append("\n");
                        }
                    }
                    if (sbOut.length() > 0) {
                        Files.write(Paths.get(ip.replace('.', '-') + ".out"), sbOut.toString().getBytes(UTF_8));
                    }
                    if (sbErr.length() > 0) {
                        Files.write(Paths.get(ip.replace('.', '-') + ".err"), sbErr.toString().getBytes(UTF_8));
                    }
                } catch (Exception e) {
                    e.printStackTrace();//todo
                }
            });


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        executorService.shutdown();
    }

    private List<String> buildCommandList() throws IOException {
        if (commandGroup.command != null)
            return Collections.singletonList(commandGroup.command);
        File file = new File(commandGroup.file);
        if (!file.exists()) {
            System.err.println("can't find file " + commandGroup.command);
            System.exit(1);
        }
        return Files.readAllLines(file.toPath(), UTF_8);
    }
}
