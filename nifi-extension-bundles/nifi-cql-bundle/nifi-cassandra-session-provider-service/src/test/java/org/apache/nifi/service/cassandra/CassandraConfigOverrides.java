/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.service.cassandra;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.UnaryOperator;

/**
 * Patches the shared {@code cassandra-base-config/cassandra.yaml} at test runtime rather than requiring a
 * full near-duplicate copy of that ~250-line file per IT scenario. {@code CassandraContainer#
 * withConfigurationOverride()} only accepts a classpath resource directory, so the patched result is
 * written to a temp file and copied into the container directly (as {@code /etc/cassandra/cassandra.yaml})
 * with {@code withCopyFileToContainer()} instead.
 */
final class CassandraConfigOverrides {

    private static final String BASE_CONFIG_RESOURCE = "cassandra-base-config/cassandra.yaml";

    static final String CONTAINER_CASSANDRA_YAML_PATH = "/etc/cassandra/cassandra.yaml";

    private CassandraConfigOverrides() {
    }

    static Path writePatchedConfig(final UnaryOperator<String> patch) {
        final String base = readBaseConfig();
        final String patched = patch.apply(base);
        if (patched.equals(base)) {
            throw new IllegalStateException("Patch did not match anything in the base Cassandra config");
        }

        try {
            final Path path = Files.createTempFile("cassandra", ".yaml");
            path.toFile().deleteOnExit();
            Files.writeString(path, patched, StandardCharsets.UTF_8);
            return path;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String readBaseConfig() {
        try (InputStream in = CassandraConfigOverrides.class.getClassLoader().getResourceAsStream(BASE_CONFIG_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource " + BASE_CONFIG_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
