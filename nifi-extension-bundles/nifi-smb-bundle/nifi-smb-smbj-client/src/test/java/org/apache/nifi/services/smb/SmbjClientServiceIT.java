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
package org.apache.nifi.services.smb;

import static java.util.stream.Collectors.toSet;
import static org.apache.nifi.services.smb.SmbjClientProviderService.DOMAIN;
import static org.apache.nifi.services.smb.SmbjClientProviderService.HOSTNAME;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PASSWORD;
import static org.apache.nifi.services.smb.SmbjClientProviderService.PORT;
import static org.apache.nifi.services.smb.SmbjClientProviderService.SHARE;
import static org.apache.nifi.services.smb.SmbjClientProviderService.USERNAME;
import static org.apache.nifi.smb.common.SmbProperties.TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.ToxiproxyContainer.ContainerProxy;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

public class SmbjClientServiceIT {

    private final static Logger sambaContainerLogger = LoggerFactory.getLogger("sambaContainer");
    private final static Logger toxyProxyLogger = LoggerFactory.getLogger("toxiProxy");

    private final Network network = Network.newNetwork();

    private final GenericContainer<?> sambaContainer = new GenericContainer<>(DockerImageName.parse("dperson/samba"))
            .withExposedPorts(445)
            .waitingFor(Wait.forListeningPort())
            .withLogConsumer(new Slf4jLogConsumer(sambaContainerLogger))
            .withNetwork(network)
            .withNetworkAliases("samba")
            .withCommand("-w domain -u username;password -s share;/folder;;no;no;username;;; -p");

    private final ToxiproxyContainer toxiproxy = new ToxiproxyContainer("shopify/toxiproxy")
            .withNetwork(network)
            .withLogConsumer(new Slf4jLogConsumer(toxyProxyLogger))
            .withNetworkAliases("toxiproxy");

    @BeforeEach
    public void beforeEach() {
        sambaContainer.start();
        toxiproxy.start();
    }

    @AfterEach
    public void afterEach() {
        toxiproxy.stop();
        sambaContainer.stop();
    }

    @Test
    public void shouldRescueAfterConnectionFailure() throws Exception {
        writeFile("testDirectory/file", "content");
        writeFile("testDirectory/directory1/file", "content");
        writeFile("testDirectory/directory2/file", "content");
        writeFile("testDirectory/directory2/nested_directory/file", "content");
        ContainerProxy sambaProxy = toxiproxy.getProxy("samba", 445);
        SmbjClientProviderService smbjClientProviderService = new SmbjClientProviderService();
        ConfigurationContext context = mock(ConfigurationContext.class);

        Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HOSTNAME, sambaProxy.getContainerIpAddress());
        properties.put(PORT, String.valueOf(sambaProxy.getProxyPort()));
        properties.put(SHARE, "share");
        properties.put(USERNAME, "username");
        properties.put(PASSWORD, "password");
        properties.put(DOMAIN, "domain");
        properties.put(TIMEOUT, "0.5 sec");
        MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null, null);

        smbjClientProviderService.onEnabled(mockConfigurationContext);

        sambaProxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, 300);

        AtomicInteger i = new AtomicInteger(0);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);
        CountDownLatch latch = new CountDownLatch(100);
        executorService.scheduleWithFixedDelay(() -> {

            int iteration = i.getAndIncrement();

            if (iteration > 100) {
                return;
            }

            executorService.submit(() -> {

                SmbClientService s = null;
                try {

                    s = smbjClientProviderService.getClient();
                    if (iteration == 25) {
                        sambaProxy.setConnectionCut(true);
                    }

                    final Set<String> actual = s.listFiles("testDirectory")
                            .map(SmbListableEntity::getIdentifier)
                            .collect(toSet());

                    assertTrue(actual.contains("testDirectory/file"));
                    assertTrue(actual.contains("testDirectory/directory1/file"));
                    assertTrue(actual.contains("testDirectory/directory2/file"));
                    assertTrue(actual.contains("testDirectory/directory2/nested_directory/file"));


                } catch (Exception e) {
                    if (iteration == 50) {
                        sambaProxy.setConnectionCut(false);
                    }
                    if (iteration == 100) {
                        fail();
                    }
                } finally {
                    if (s != null) {
                        try {
                            s.close();
                        } catch (Exception e) {
                        }
                    }
                }
                latch.countDown();
            });

        }, 0, 2, TimeUnit.SECONDS);

        latch.await();
        executorService.shutdown();
        smbjClientProviderService.onDisabled();
    }

    private void writeFile(String path, String content) {
        String containerPath = "/folder/" + path;
        sambaContainer.copyFileToContainer(Transferable.of(content), containerPath);
    }

}
