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

package org.apache.nifi.minifi.c2.command;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.util.NiFiProperties;

public class TransferDebugCommandHelper {

    private static final String MINIFI_CONFIG_FILE_PATH = "nifi.minifi.config.file";
    private static final String MINIFI_BOOTSTRAP_FILE_PATH = "nifi.minifi.bootstrap.file";
    private static final String MINIFI_LOG_DIRECTORY = "nifi.minifi.log.directory";
    private static final String MINIFI_APP_LOG_FILE = "nifi.minifi.app.log.file";
    private static final String MINIFI_BOOTSTRAP_LOG_FILE = "nifi.minifi.bootstrap.log.file";

    private static final Set<String> SENSITIVE_PROPERTY_KEYWORDS =
        Stream.of("key:", "algorithm:", "secret.key", "sensitive.props.key", "sensitive.props.algorithm", "secret", "password", "passwd")
            .map(String::toLowerCase)
            .collect(collectingAndThen(toSet(), Collections::unmodifiableSet));

    private final NiFiProperties niFiProperties;

    public TransferDebugCommandHelper(NiFiProperties niFiProperties) {
        this.niFiProperties = niFiProperties;
    }

    public List<Path> debugBundleFiles() {
        return Stream.of(
                Paths.get(niFiProperties.getProperty(MINIFI_CONFIG_FILE_PATH)),
                Paths.get(niFiProperties.getProperty(MINIFI_BOOTSTRAP_FILE_PATH)),
                Paths.get(niFiProperties.getProperty(MINIFI_LOG_DIRECTORY), niFiProperties.getProperty(MINIFI_APP_LOG_FILE)),
                Paths.get(niFiProperties.getProperty(MINIFI_LOG_DIRECTORY), niFiProperties.getProperty(MINIFI_BOOTSTRAP_LOG_FILE)))
            .filter(Files::exists)
            .filter(Files::isRegularFile)
            .collect(toList());
    }

    public boolean excludeSensitiveText(String text) {
        return ofNullable(text)
            .map(String::toLowerCase)
            .map(t -> SENSITIVE_PROPERTY_KEYWORDS.stream().noneMatch(keyword -> t.contains(keyword)))
            .orElse(true);
    }
}
