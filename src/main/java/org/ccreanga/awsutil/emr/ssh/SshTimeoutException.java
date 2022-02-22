package org.ccreanga.awsutil.emr.ssh;


import java.util.concurrent.TimeoutException;
public final class SshTimeoutException extends TimeoutException {


    public SshTimeoutException(String cmd, String host, long timeout) {
        super("Command '" + cmd + "' on host " + host + " timed out after " + timeout + " seconds");
    }

}