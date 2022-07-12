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
package org.apache.nifi.processors.smb;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class NiFiSmbClientIT {

    private final static Logger logger = LoggerFactory.getLogger(NiFiSmbClientIT.class);

    private final GenericContainer<?> sambaContainer = new GenericContainer<>(DockerImageName.parse("dperson/samba"))
            .withExposedPorts(139, 445)
            .waitingFor(Wait.forListeningPort())
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withCommand("-w domain -u username;password -s share;/folder;;no;no;username;;; -p");
    private final SMBClient smbClient = new SMBClient();
    private final AuthenticationContext authenticationContext =
            new AuthenticationContext("username", "password".toCharArray(), "domain");
    private NiFiSmbClient niFiSmbjClient;

    @BeforeEach
    public void beforeEach() throws Exception {
        sambaContainer.start();
        niFiSmbjClient = createClient();
    }

    @AfterEach
    public void afterEach() {
        niFiSmbjClient.close();
        sambaContainer.stop();
    }

    @Test
    public void shouldIterateDirectory() {
        niFiSmbjClient.createDirectory("testDirectory\\directory1");
        niFiSmbjClient.createDirectory("testDirectory\\directory2");
        niFiSmbjClient.createDirectory("testDirectory\\directory2\\nested_directory");
        writeFile("testDirectory\\file", "content");
        writeFile("testDirectory\\directory1\\file", "content");
        writeFile("testDirectory\\directory2\\file", "content");
        writeFile("testDirectory\\directory2\\nested_directory\\file", "content");
        final Set<String> actual = niFiSmbjClient.listRemoteFiles("testDirectory")
                .map(SmbListableEntity::getIdentifier)
                .collect(toSet());
        assertTrue(actual.contains("testDirectory\\file"));
        assertTrue(actual.contains("testDirectory\\directory1\\file"));
        assertTrue(actual.contains("testDirectory\\directory2\\file"));
        assertTrue(actual.contains("testDirectory\\directory2\\nested_directory\\file"));
    }

    private void writeFile(String path, String content) {
        try (OutputStream outputStream = niFiSmbjClient.getOutputStreamForFile(path)) {
            outputStream.write(content.getBytes());
            outputStream.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private NiFiSmbClient createClient() throws IOException {
        return new NiFiSmbClientFactory().create(
                smbClient.connect(sambaContainer.getHost(), sambaContainer.getMappedPort(445)).authenticate(authenticationContext), "share");
    }

}
