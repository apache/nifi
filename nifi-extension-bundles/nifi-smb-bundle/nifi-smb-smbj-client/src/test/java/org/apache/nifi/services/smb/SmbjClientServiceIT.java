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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
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
        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        final Proxy proxy = toxiproxyClient.createProxy("samba", "0.0.0.0:8666", "samba:445");
        final String ipAddressViaToxiproxy = toxiproxy.getHost();
        final int portViaToxiproxy = toxiproxy.getMappedPort(8666);
        SmbjClientProviderService smbjClientProviderService = new SmbjClientProviderService();

        Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HOSTNAME, ipAddressViaToxiproxy);
        properties.put(PORT, String.valueOf(portViaToxiproxy));
        properties.put(SHARE, "share");
        properties.put(USERNAME, "username");
        properties.put(PASSWORD, "password");
        properties.put(DOMAIN, "domain");
        properties.put(TIMEOUT, "0.5 sec");
        MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null, null);

        smbjClientProviderService.onEnabled(mockConfigurationContext);

        proxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, 300);

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

                    s = smbjClientProviderService.getClient(mock(ComponentLog.class));
                    if (iteration == 25) {
                        proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0L);
                        proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0L);
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
                        try {
                            proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
                            proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    if (iteration == 100) {
                        fail();
                    }
                } finally {
                    if (s != null) {
                        try {
                            s.close();
                        } catch (Exception ignored) {
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

    @Test
    public void shouldContinueListingAfterPermissionDenied() throws Exception {
        writeFile("testDirectory/directory1/file1", "content");
        writeFile("testDirectory/directory2/file2", "content");
        writeFile("testDirectory/directory3/file3", "content");

        sambaContainer.execInContainer("bash", "-c", "chmod 000 /folder/testDirectory/directory2");

        SmbjClientProviderService smbjClientProviderService = new SmbjClientProviderService();

        Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HOSTNAME, sambaContainer.getHost());
        properties.put(PORT, String.valueOf(sambaContainer.getMappedPort(445)));
        properties.put(SHARE, "share");
        properties.put(USERNAME, "username");
        properties.put(PASSWORD, "password");
        properties.put(DOMAIN, "domain");
        properties.put(TIMEOUT, "0.5 sec");

        MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null, null);
        smbjClientProviderService.onEnabled(mockConfigurationContext);

        SmbClientService smbClientService = smbjClientProviderService.getClient(mock(ComponentLog.class));

        final Set<String> actual = smbClientService.listFiles("testDirectory")
                .map(SmbListableEntity::getIdentifier)
                .collect(toSet());

        assertEquals(2, actual.size());
        assertTrue(actual.contains("testDirectory/directory1/file1"));
        assertTrue(actual.contains("testDirectory/directory3/file3"));

        smbjClientProviderService.onDisabled();
    }

    private void writeFile(String path, String content) {
        String containerPath = "/folder/" + path;
        sambaContainer.copyFileToContainer(Transferable.of(content), containerPath);
    }

}
