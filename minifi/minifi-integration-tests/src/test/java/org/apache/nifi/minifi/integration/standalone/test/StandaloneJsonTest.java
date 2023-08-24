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


import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;
import org.apache.nifi.minifi.integration.util.LogUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class StandaloneJsonTest {

    static Stream<Arguments> verifyLogEntries() {
        return Stream.of("CsvToJson", "DecompressionCircularFlow", "MiNiFiTailLogAttribute",
                "MiNiFiTailLogAttribute", "ReplaceTextExpressionLanguageCSVReformatting",
                "MultipleRelationships", "ProcessGroups", "StressTestFramework")
            .map(Arguments::of);
    }

    DockerComposeExtension docker;

    @AfterEach
    void stopDocker() {
        docker.after();
    }

    @ParameterizedTest(name = "{index}: Name: {0}")
    @MethodSource
    public void verifyLogEntries(String name) throws Exception {
        setDocker(name);
        LogUtil.verifyLogEntries(getExpectedJson(name), docker.containers().container("minifi"));
    }

    private void setDocker(String name) throws Exception {
        String dockerComposeYmlFile = "target/test-classes/docker-compose-" + name + "Test.yml";
        try (InputStream inputStream = StandaloneJsonTest.class.getClassLoader().getResourceAsStream("docker-compose-standalone.yml");
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
             FileWriter fileWriter = new FileWriter(dockerComposeYmlFile);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line.replace("REPLACED_WITH_CONFIG_FILE", getFlowConfigPath(name)));
                bufferedWriter.newLine();
            }
        }
        docker = DockerComposeExtension.builder()
            .file(dockerComposeYmlFile)
            .waitingForService("minifi", HealthChecks.toRespond2xxOverHttp(8000, dockerPort -> "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort()))
            .build();
        docker.before();
    }

    private String getFlowConfigPath(String name) {
        return "./standalone/" + name + "/" + name + ".json";
    }

    private String getExpectedJson(String name) {
        return "standalone/" + name + "/expected.json";
    }
}