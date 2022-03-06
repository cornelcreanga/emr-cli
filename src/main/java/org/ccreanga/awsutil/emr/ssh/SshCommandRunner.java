package org.ccreanga.awsutil.emr.ssh;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.SECONDS;

public class SshCommandRunner implements Callable<List<SshResponse>> {

    private final SshClient client;
    private final SshConnection conn;
    private final List<String> cmd;
    private final long timeout;

    public SshCommandRunner(SshClient client, SshConnection conn, List<String> cmd, long timeout) {
        this.client = client;
        this.conn = conn;
        this.cmd = cmd;
        this.timeout = timeout;
    }

    @Override
    public List<SshResponse> call() throws SshTimeoutException, IOException {
        System.out.println("Trying to connect to "+conn.getHostname());
        //todo - allow custom location for user keys
        ConnectFuture cf = client.connect(conn.getUsername(), conn.getHostname(), 22);
        System.out.println("Connected to "+conn.getHostname());
        ClientSession session = cf.verify().getSession();

        session.auth().verify(SECONDS.toMillis(timeout));
        List<SshResponse> responses = new ArrayList<>();
        for (String command : cmd) {
            ChannelExec channelExec = session.createExecChannel(command);
            ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
            ByteArrayOutputStream err = new ByteArrayOutputStream(1024);
            channelExec.setOut(out);
            channelExec.setErr(err);


            channelExec.open();
            Set<ClientChannelEvent> events = channelExec.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), SECONDS.toMillis(timeout));


            if (events.contains(ClientChannelEvent.TIMEOUT)) {
                throw new SshTimeoutException(command, conn.getHostname(), timeout);
            }
            responses.add(new SshResponse(out.toString(), err.toString(), channelExec.getExitStatus()));
            channelExec.close();
        }
        session.close(false);

        return responses;


    }

}
