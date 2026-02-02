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
package org.apache.nifi.controller.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * System Test implementation of Component Metric Reporter
 */
public class SystemTestComponentMetricReporter implements ComponentMetricReporter {
    private static final String USER_DIR_PROPERTY = "user.dir";

    private static final Path USER_DIRECTORY = Paths.get(System.getProperty(USER_DIR_PROPERTY));

    private static final String LOG_FILE_PREFIX = SystemTestComponentMetricReporter.class.getName();

    private static final Logger logger = LoggerFactory.getLogger(SystemTestComponentMetricReporter.class);

    @Override
    public void recordGauge(final GaugeRecord gaugeRecord) {
        logger.info("Recording Gauge [{}] Value [{}]", gaugeRecord.name(), gaugeRecord.value());

        final ComponentMetricContext componentMetricContext = gaugeRecord.componentMetricContext();
        final String formatted = "Gauge [%s] Value [%s] Component ID [%s]".formatted(gaugeRecord.name(), gaugeRecord.value(), componentMetricContext.id());
        final String filename = "%s.GaugeRecord.%d.log".formatted(LOG_FILE_PREFIX, System.nanoTime());
        final Path log = USER_DIRECTORY.resolve(filename);

        try {
            Files.writeString(log, formatted);
        } catch (final Exception e) {
            logger.warn("Failed to write Gauge Record [{}]", log, e);
        }
    }

    @Override
    public void recordCounter(final CounterRecord counterRecord) {
        logger.info("Recording Counter [{}] Value [{}]", counterRecord.name(), counterRecord.value());

        final ComponentMetricContext componentMetricContext = counterRecord.componentMetricContext();
        final String formatted = "Counter [%s] Value [%s] Component ID [%s]".formatted(counterRecord.name(), counterRecord.value(), componentMetricContext.id());
        final String filename = "%s.CounterRecord.%d.log".formatted(LOG_FILE_PREFIX, System.nanoTime());
        final Path log = USER_DIRECTORY.resolve(filename);

        try {
            Files.writeString(log, formatted);
        } catch (final Exception e) {
            logger.warn("Failed to write Counter Record [{}]", log, e);
        }
    }

    @Override
    public void close() {
        logger.info("Clearing Component Metrics from User Directory [{}]", USER_DIRECTORY);
        try (Stream<Path> userDirectoryPaths = Files.list(USER_DIRECTORY)) {
            userDirectoryPaths
                    .filter(path -> path.getFileName().toString().startsWith(LOG_FILE_PREFIX))
                    .forEach(log -> {
                        try {
                            Files.delete(log);
                        } catch (final Exception e) {
                            logger.warn("Delete Component Metrics Log [{}] failed", log, e);
                        }
                    });
        } catch (final Exception e) {
            logger.warn("Failed to clear User Directory [{}]", USER_DIRECTORY, e);
        }
    }
}
