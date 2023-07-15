package org.apache.nifi.redis.testcontainers;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RedisSentinelContainer extends RedisContainer {

    public static final int REDIS_SENTINEL_PORT = 26379;

    public RedisSentinelContainer(final @NonNull DockerImageName dockerImageName) {
        super(dockerImageName);

        setPort(REDIS_SENTINEL_PORT);
    }

    public RedisSentinelContainer(final @NonNull String fullImageName) {
        this(DockerImageName.parse(fullImageName));
    }

    @NonNull
    protected String masterHost = "localhost";
    protected int masterPort = REDIS_PORT;
    @NonNull
    protected String masterName = "mymaster";
    @Nullable
    protected String sentinelPassword = null;
    private long downAfterMilliseconds = 60000L;
    private long failoverTimeout = 180000L;
    private int parallelSyncs = 1;
    private int quorumSize = 1;

    public void setMasterHost(final @NonNull String masterHost) {
        this.masterHost = masterHost;
    }

    public void setMasterPort(final int masterPort) {
        this.masterPort = masterPort;
    }

    public void setMasterName(final @NonNull String masterName) {
        this.masterName = masterName;
    }

    public void setSentinelPassword(final @Nullable String sentinelPassword) {
        this.sentinelPassword = sentinelPassword;
    }

    public void setQuorumSize(final int quorumSize) {
        this.quorumSize = quorumSize;
    }

    public void setDownAfterMilliseconds(final long downAfterMilliseconds) {
        this.downAfterMilliseconds = downAfterMilliseconds;
    }

    public void setFailoverTimeout(final long failoverTimeout) {
        this.failoverTimeout = failoverTimeout;
    }

    public void setParallelSyncs(final int parallelSyncs) {
        this.parallelSyncs = parallelSyncs;
    }


    @Override
    protected void adjustConfiguration() {
        addConfigurationOption("port " + port);

        addConfigurationOption(String.format("sentinel monitor %s %s %d %d", masterName, masterHost, masterPort, quorumSize));
        addConfigurationOption(String.format("sentinel down-after-milliseconds %s %d", masterName, downAfterMilliseconds));
        addConfigurationOption(String.format("sentinel failover-timeout %s %d", masterName, failoverTimeout));
        addConfigurationOption(String.format("sentinel parallel-syncs %s %d", masterName, parallelSyncs));

        if (password != null) {
            addConfigurationOption("sentinel auth-pass " + masterName + " " + password);
        }
        if (sentinelPassword != null) {
            addConfigurationOption("requirepass " + sentinelPassword);
            addConfigurationOption("sentinel sentinel-pass " + sentinelPassword);
        }
    }

    @Override
    protected void configure() {
        super.configure();

        System.err.println("SENTINEL AT PORT " + port);
        System.err.println(configurationOptions);

        List<String> commandParts = new ArrayList<>(Arrays.asList(getCommandParts()));
        commandParts.add("--sentinel");
        setCommandParts(commandParts.toArray(new String[0]));
    }
}
