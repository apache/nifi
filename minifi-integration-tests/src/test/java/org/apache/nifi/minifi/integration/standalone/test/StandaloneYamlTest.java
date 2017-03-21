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

package org.apache.nifi.minifi.integration.standalone.test;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class StandaloneYamlTest {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneYamlTest.class);

    protected final String version;
    protected final String name;

    @Parameterized.Parameters(name = "{index}: Schema Version: {0} Name: {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v1", "CsvToJson"},
                {"v1", "DecompressionCircularFlow"},
                {"v1", "MiNiFiTailLogAttribute"},
                {"v1", "ReplaceTextExpressionLanguageCSVReformatting"},
                {"v2", "MultipleRelationships"},
                {"v2", "ProcessGroups"},
                {"v2", "StressTestFramework"}
        });
    }

    @Rule
    public DockerComposeRule dockerComposeRule;

    public StandaloneYamlTest(String version, String name) throws IOException {
        this.version = version;
        this.name = name;
        String dockerComposeYmlFile = "target/test-classes/docker-compose-" + version + "-" + name + "Test-yml.yml";
        try (InputStream inputStream = StandaloneYamlTest.class.getClassLoader().getResourceAsStream("docker-compose-v1-standalone.yml");
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
             FileWriter fileWriter = new FileWriter(dockerComposeYmlFile);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line.replace("REPLACED_WITH_CONFIG_FILE", getConfigYml()));
                bufferedWriter.newLine();
            }
        }
        dockerComposeRule = DockerComposeRule.builder()
                .file(dockerComposeYmlFile)
                .waitingForService("minifi", HealthChecks.toRespond2xxOverHttp(8000, dockerPort -> "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort()))
                .build();
    }

    protected String getConfigYml() {
        return "./standalone/" + version + "/" + name + "/yml/" + name + ".yml";
    }

    protected String getExpectedJson() {
        return "standalone/" + version + "/" + name + "/yml/expected.json";
    }

    @Test(timeout = 60_000)
    public void verifyLogEntries() throws IOException, InterruptedException, ExecutionException {
        Pattern expectedLine;
        int expectedOccurences;
        try (InputStream inputStream = StandaloneYamlTest.class.getClassLoader().getResourceAsStream(getExpectedJson())) {
            Map<String, Object> map = new ObjectMapper().readValue(inputStream, Map.class);
            expectedLine = Pattern.compile((String) map.get("pattern"));
            expectedOccurences = (int) map.getOrDefault("occurrences", 1);
        }
        DockerPort dockerPort = dockerComposeRule.containers().container("minifi").port(8000);
        URL url = new URL("http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort());
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        try (InputStream inputStream = urlConnection.getInputStream();
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            String line;
            int occurrences = 0;
            while ((line = bufferedReader.readLine()) != null) {
                if (expectedLine.matcher(line).find()) {
                    logger.info("Found expected: " + line);
                    if (++occurrences >= expectedOccurences) {
                        logger.info("Found target " + occurrences + " times");
                        return;
                    }
                }
            }
            fail("End of log reached without " + expectedOccurences + " match(es)");
        } finally {
            urlConnection.disconnect();
        }
    }
}