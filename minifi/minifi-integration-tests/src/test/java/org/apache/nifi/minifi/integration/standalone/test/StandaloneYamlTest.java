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
import org.apache.nifi.minifi.integration.util.LogUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

public class StandaloneYamlTest {

    static Stream<Arguments> verifyLogEntries() {
        return Stream.of(
                Arguments.of("v1", "CsvToJson"),
                Arguments.of("v1", "DecompressionCircularFlow"),
                Arguments.of("v1", "MiNiFiTailLogAttribute"),
                Arguments.of("v1", "ReplaceTextExpressionLanguageCSVReformatting"),
                Arguments.of("v2", "MultipleRelationships"),
                Arguments.of("v2", "ProcessGroups"),
                Arguments.of("v2", "StressTestFramework")
        );
    }

    DockerComposeExtension docker;

    @AfterEach
    void stopDocker() {
        docker.after();
    }

    public void setDocker(final String version, final String name) throws Exception {
        String dockerComposeYmlFile = "target/test-classes/docker-compose-" + version + "-" + name + "Test-yml.yml";
        try (InputStream inputStream = StandaloneYamlTest.class.getClassLoader().getResourceAsStream("docker-compose-v1-standalone.yml");
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
             FileWriter fileWriter = new FileWriter(dockerComposeYmlFile);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line.replace("REPLACED_WITH_CONFIG_FILE", getConfigYml(version, name)));
                bufferedWriter.newLine();
            }
        }
        docker = DockerComposeExtension.builder()
                .file(dockerComposeYmlFile)
                .waitingForService("minifi", HealthChecks.toRespond2xxOverHttp(8000, dockerPort -> "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort()))
                .build();
        docker.before();
    }

    protected String getConfigYml(final String version, final String name) {
        return "./standalone/" + version + "/" + name + "/yml/" + name + ".yml";
    }

    protected String getExpectedJson(final String version, final String name) {
        return "standalone/" + version + "/" + name + "/yml/expected.json";
    }

    @ParameterizedTest(name = "{index}: Schema Version: {0} Name: {1}")
    @MethodSource
    public void verifyLogEntries(final String version, final String name) throws Exception {
        setDocker(version, name);
        LogUtil.verifyLogEntries(getExpectedJson(version, name), docker.containers().container("minifi"));
    }
}