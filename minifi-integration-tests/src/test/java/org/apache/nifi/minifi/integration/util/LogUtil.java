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

package org.apache.nifi.minifi.integration.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

public class LogUtil {
    private static final Logger logger = LoggerFactory.getLogger(LogUtil.class);

    public static void verifyLogEntries(String expectedJsonFilename, Container container) throws Exception {
        List<ExpectedLogEntry> expectedLogEntries;
        try (InputStream inputStream = LogUtil.class.getClassLoader().getResourceAsStream(expectedJsonFilename)) {
            List<Map<String, Object>> expected = new ObjectMapper().readValue(inputStream, List.class);
            expectedLogEntries = expected.stream().map(map -> new ExpectedLogEntry(Pattern.compile((String)map.get("pattern")), (int) map.getOrDefault("occurrences", 1))).collect(Collectors.toList());
        }
        DockerPort dockerPort = container.port(8000);
        URL url = new URL("http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort());
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        try (InputStream inputStream = urlConnection.getInputStream();
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            String line;
            for (ExpectedLogEntry expectedLogEntry : expectedLogEntries) {
                boolean satisfied = false;
                int occurrences = 0;
                while ((line = bufferedReader.readLine()) != null) {
                    if (expectedLogEntry.pattern.matcher(line).find()) {
                        logger.info("Found expected: " + line);
                        if (++occurrences >= expectedLogEntry.numOccurrences) {
                            logger.info("Found target " + occurrences + " times");
                            satisfied = true;
                            break;
                        }
                    }
                }
                if (!satisfied) {
                    fail("End of log reached without " + expectedLogEntry.numOccurrences + " match(es) of " + expectedLogEntry.pattern);
                }
            }
        } finally {
            urlConnection.disconnect();
        }
    }

    private static class ExpectedLogEntry {
        private final Pattern pattern;
        private final int numOccurrences;

        private ExpectedLogEntry(Pattern pattern, int numOccurrences) {
            this.pattern = pattern;
            this.numOccurrences = numOccurrences;
        }
    }
}
