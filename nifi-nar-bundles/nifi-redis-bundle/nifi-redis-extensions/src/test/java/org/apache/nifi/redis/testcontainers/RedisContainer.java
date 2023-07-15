package org.apache.nifi.redis.testcontainers;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RedisContainer extends GenericContainer<RedisContainer> {

    public static final int REDIS_PORT = 6379;

    public RedisContainer(@NonNull DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public RedisContainer(@NonNull String fullImageName) {
        this(DockerImageName.parse(fullImageName));
    }

    public int port = REDIS_PORT;

    @Nullable
    protected String password = null;
    @Nullable
    protected Path configurationMountDirectory = null;

    protected final List<String> configurationOptions = new ArrayList<>();

    public void setPassword(final @Nullable String password) {
        this.password = password;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public void mountConfigurationFrom(final Path mountDirectory) {
        this.configurationMountDirectory = mountDirectory;
    }

    public void addConfigurationOption(final String configurationOption) {
        this.configurationOptions.add(configurationOption);
    }

    protected void adjustConfiguration() {
        addConfigurationOption("port " + port);

        if (password != null) {
            addConfigurationOption("requirepass " + password);
        }
    }

    /**
     * Sets up a static binding between a port on the host and one in the container.
     * In order for auto-discovery mechanisms of Redis to work, 1-to-1 mapped ports are useful.
     */
    public void addPortBinding(int hostPort, int containerPort) {
        addFixedExposedPort(hostPort, containerPort);
    }

    @Override
    protected void configure() {
        adjustConfiguration();

        Path configurationFilePath = writeConfigurationFile().toAbsolutePath();
        String hostPath = configurationFilePath.toString();
        String containerPath = "/usr/local/etc/redis/redis.conf";
        addFileSystemBind(hostPath, containerPath, BindMode.READ_WRITE);

        setCommand(containerPath);
    }

    protected Path writeConfigurationFile() {
        try {
            Path mountDirectory = this.configurationMountDirectory;
            if (mountDirectory == null) {
                mountDirectory = Files.createTempDirectory("redis-container-configuration");
            }

            Path configFile = mountDirectory.resolve("redis-" + UUID.randomUUID() + ".conf");
            Files.write(configFile, configurationOptions, StandardCharsets.UTF_8);

            return configFile;
        } catch (IOException ioException) {
            throw new IllegalStateException("Cannot start container because configuration could not be written!", ioException);
        }
    }
}
