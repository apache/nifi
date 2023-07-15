package org.apache.nifi.redis.testcontainers;

import org.springframework.lang.NonNull;
import org.testcontainers.utility.DockerImageName;

public class RedisReplicaContainer extends RedisContainer {

    public RedisReplicaContainer(@NonNull DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public RedisReplicaContainer(@NonNull String fullImageName) {
        this(DockerImageName.parse(fullImageName));
    }

    @NonNull
    protected String masterHost = "localhost";
    protected int masterPort = REDIS_PORT;

    public void setMasterHost(@NonNull String masterHost) {
        this.masterHost = masterHost;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public void setReplicaOf(@NonNull String masterHost, int masterPort) {
        setMasterHost(masterHost);
        setMasterPort(masterPort);
    }

    @Override
    protected void adjustConfiguration() {
        addConfigurationOption("port " + port);
        
        if (password != null) {
            addConfigurationOption("requirepass " + password);
            addConfigurationOption("masterauth " + password);
        }

        addConfigurationOption("replicaof " + masterHost + " " + masterPort);
    }
}
