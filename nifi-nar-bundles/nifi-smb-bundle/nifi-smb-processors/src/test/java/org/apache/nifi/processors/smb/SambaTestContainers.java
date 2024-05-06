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

import static java.util.Arrays.fill;
import static org.apache.nifi.processors.smb.ListSmb.SMB_CLIENT_PROVIDER_SERVICE;
import static org.apache.nifi.services.smb.SmbjClientProviderService.DOMAIN;
import static org.apache.nifi.services.smb.SmbjClientProviderService.HOSTNAME;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PASSWORD;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PORT;
import static org.apache.nifi.services.smb.SmbjClientProviderService.SHARE;
import static org.apache.nifi.services.smb.SmbjClientProviderService.USERNAME;

import org.apache.nifi.services.smb.SmbjClientProviderService;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

public class SambaTestContainers {

    protected final static Integer DEFAULT_SAMBA_PORT = 445;
    protected final static Logger logger = LoggerFactory.getLogger(SambaTestContainers.class);
    protected final GenericContainer<?> sambaContainer = new GenericContainer<>(DockerImageName.parse("dperson/samba"))
            .withExposedPorts(DEFAULT_SAMBA_PORT, 139)
            .waitingFor(Wait.forListeningPort())
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withCommand("-w domain -u username;password -s share;/folder;;no;no;username;;; -p");

    @BeforeEach
    public void beforeEach() {
        sambaContainer.start();
    }

    @AfterEach
    public void afterEach() {
        sambaContainer.stop();
    }

    protected SmbjClientProviderService configureSmbClient(TestRunner testRunner, boolean shouldEnableSmbClient)
            throws Exception {
        final SmbjClientProviderService smbjClientProviderService = new SmbjClientProviderService();
        testRunner.addControllerService("client-provider", smbjClientProviderService);
        testRunner.setProperty(SMB_CLIENT_PROVIDER_SERVICE, "client-provider");
        testRunner.setProperty(smbjClientProviderService, HOSTNAME, sambaContainer.getHost());
        testRunner.setProperty(smbjClientProviderService, PORT,
                String.valueOf(sambaContainer.getMappedPort(DEFAULT_SAMBA_PORT)));
        testRunner.setProperty(smbjClientProviderService, USERNAME, "username");
        testRunner.setProperty(smbjClientProviderService, PASSWORD, "password");
        testRunner.setProperty(smbjClientProviderService, SHARE, "share");
        testRunner.setProperty(smbjClientProviderService, DOMAIN, "domain");
        if (shouldEnableSmbClient) {
            testRunner.enableControllerService(smbjClientProviderService);
        }
        return smbjClientProviderService;
    }

    protected String generateContentWithSize(int sizeInBytes) {
        byte[] bytes = new byte[sizeInBytes];
        fill(bytes, (byte) 1);
        return new String(bytes);
    }

    protected void writeFile(String path, String content) {
        String containerPath = "/folder/" + path;
        sambaContainer.copyFileToContainer(Transferable.of(content), containerPath);
    }

}
