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

package org.apache.nifi.minifi.bootstrap.service;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GracefulShutdownParameterProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(GracefulShutdownParameterProvider.class);
    private static final String GRACEFUL_SHUTDOWN_PROP = "graceful.shutdown.seconds";
    private static final String DEFAULT_GRACEFUL_SHUTDOWN_VALUE = "20";
    private static final String INVALID_GRACEFUL_SHUTDOWN_SECONDS_MESSAGE =
        "The {} property in Bootstrap Config File has an invalid value. Must be a non-negative integer, Falling back to the default {} value";

    private final BootstrapFileProvider bootstrapFileProvider;

    public GracefulShutdownParameterProvider(BootstrapFileProvider bootstrapFileProvider) {
        this.bootstrapFileProvider = bootstrapFileProvider;
    }

    public int getGracefulShutdownSeconds() throws IOException {
        Properties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();

        String gracefulShutdown = bootstrapProperties.getProperty(GRACEFUL_SHUTDOWN_PROP, DEFAULT_GRACEFUL_SHUTDOWN_VALUE);

        int gracefulShutdownSeconds;
        try {
            gracefulShutdownSeconds = Integer.parseInt(gracefulShutdown);
        } catch (NumberFormatException nfe) {
            gracefulShutdownSeconds = Integer.parseInt(DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
            LOGGER.warn(INVALID_GRACEFUL_SHUTDOWN_SECONDS_MESSAGE, GRACEFUL_SHUTDOWN_PROP, gracefulShutdownSeconds);
        }

        if (gracefulShutdownSeconds < 0) {
            gracefulShutdownSeconds = Integer.parseInt(DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
            LOGGER.warn(INVALID_GRACEFUL_SHUTDOWN_SECONDS_MESSAGE, GRACEFUL_SHUTDOWN_PROP, gracefulShutdownSeconds);
        }
        return gracefulShutdownSeconds;
    }
}
