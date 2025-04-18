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
package org.apache.nifi.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

/**
 * System Test Flow Action Reporter writes serialized Flow Action Attributes to files in the current user directory
 */
public class SystemTestFlowActionReporter implements FlowActionReporter {
    private static final String USER_DIR_PROPERTY = "user.dir";

    private static final Path USER_DIRECTORY = Paths.get(System.getProperty(USER_DIR_PROPERTY));

    private static final String LOG_FILE_PREFIX = SystemTestFlowActionReporter.class.getName();

    private static final Logger logger = LoggerFactory.getLogger(SystemTestFlowActionReporter.class);

    @Override
    public void reportFlowActions(final Collection<FlowAction> actions) {
        logger.info("Reporting Flow Actions [{}]", actions.size());
        for (final FlowAction flowAction : actions) {
            final Map<String, String> attributes = flowAction.getAttributes();
            final String formatted = attributes.toString();

            final String filename = "%s.%d.log".formatted(LOG_FILE_PREFIX, System.nanoTime());
            final Path log = USER_DIRECTORY.resolve(filename);

            try {
                Files.writeString(log, formatted);
            } catch (final Exception e) {
                logger.warn("Failed to write Flow Action [{}]", log, e);
            }
        }
    }

    @Override
    public void close() {
        logger.info("Clearing Flow Actions from User Directory [{}]", USER_DIRECTORY);
        try (Stream<Path> userDirectoryPaths = Files.list(USER_DIRECTORY)) {
            userDirectoryPaths
                    .filter(path -> path.getFileName().toString().startsWith(LOG_FILE_PREFIX))
                    .forEach(log -> {
                        try {
                            Files.delete(log);
                        } catch (final Exception e) {
                            logger.warn("Delete Flow Action Log [{}] failed", log, e);
                        }
                    });
        } catch (final Exception e) {
            logger.warn("Failed to clear User Directory [{}]", USER_DIRECTORY, e);
        }
    }
}
