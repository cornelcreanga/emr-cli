package org.ccreanga.awsutil.emr.ssh;

public final class SshResponse {

    private final String stdOutput;
    private final String errOutput;
    private final int returnCode;

    SshResponse(String stdOutput, String errOutput, int returnCode) {
        this.stdOutput = stdOutput;
        this.errOutput = errOutput;
        this.returnCode = returnCode;
    }

    public String getStdOutput() {
        return stdOutput;
    }

    public String getErrOutput() {
        return errOutput;
    }

    public int getReturnCode() {
        return returnCode;
    }

}
