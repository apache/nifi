/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class AbstractMongoIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMongoIT.class);
    private static final String DOCKER_IMAGE = System.getProperty("mongo.docker.image", "mongo:8");
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
    private static final long PING_TIMEOUT_SECONDS = 30;
    protected static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(DockerImageName.parse(DOCKER_IMAGE))
            // Let glibc own rseq so TCMalloc does not use incompatible per-CPU caches on newer kernels.
            .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .waitingFor(Wait.forLogMessage("(?i).*waiting for connections.*", 1).withStartupTimeout(STARTUP_TIMEOUT));

    @BeforeAll
    public static void start() {
        try {
            MONGO_CONTAINER.start();

            final MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO_CONTAINER.getConnectionString()))
                    .applyToClusterSettings(builder -> builder.serverSelectionTimeout(PING_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .applyToSocketSettings(builder -> builder
                            .connectTimeout(PING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .readTimeout(PING_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .build();
            try (final MongoClient client = MongoClients.create(settings)) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
            }
        } catch (final RuntimeException e) {
            logContainerDiagnostics(e);
            throw e;
        }
    }

    @AfterAll
    public static void stop() {
        MONGO_CONTAINER.stop();
    }

    private static void logContainerDiagnostics(final RuntimeException startupException) {
        final String containerId = MONGO_CONTAINER.getContainerId();
        if (containerId == null) {
            LOGGER.error("MongoDB container [{}] is unavailable for inspection after startup failure", DOCKER_IMAGE);
            return;
        }

        try {
            final InspectContainerResponse.ContainerState state = MONGO_CONTAINER.getDockerClient().inspectContainerCmd(containerId).exec().getState();
            LOGGER.error("MongoDB container [{}] failed to become ready: status [{}] exit code [{}] OOM killed [{}] error [{}]",
                    containerId, state.getStatus(), state.getExitCodeLong(), state.getOOMKilled(), state.getError());
            LOGGER.error("MongoDB container [{}] logs:\n{}", containerId, MONGO_CONTAINER.getLogs());
        } catch (final RuntimeException diagnosticsException) {
            startupException.addSuppressed(diagnosticsException);
        }
    }
}
