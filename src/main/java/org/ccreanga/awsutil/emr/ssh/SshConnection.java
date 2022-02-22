package org.ccreanga.awsutil.emr.ssh;

public class SshConnection {

    private String username;
    private String password;
    private String hostname;
    private byte[] key;

    public SshConnection(String username, String hostname) {
        this.username = username;
        this.hostname = hostname;
    }

    public SshConnection(String username, String password, String hostname) {
        this.username = username;
        this.password = password;
        this.hostname = hostname;
    }

    public SshConnection(String hostname, byte[] key) {
        this.hostname = hostname;
        this.key = key;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHostname() {
        return hostname;
    }

    public byte[] getKey() {
        return key;
    }
}
