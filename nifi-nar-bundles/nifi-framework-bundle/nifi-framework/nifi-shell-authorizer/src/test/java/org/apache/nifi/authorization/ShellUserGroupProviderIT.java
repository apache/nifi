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
package org.apache.nifi.authorization;

import java.io.File;
import java.io.IOException;

import org.apache.nifi.util.MockPropertyValue;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.mockito.Mockito;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import org.apache.nifi.authorization.util.ShellRunner;


public class ShellUserGroupProviderIT extends ShellUserGroupProviderBase {
    private static final Logger logger = LoggerFactory.getLogger(ShellUserGroupProviderIT.class);

    // These images are publicly available on the hub.docker.com, and the source to each
    // is available on github.  In lieu of using named images, the Dockerfiles could be
    // migrated into module and referenced in the testcontainer setup.
    private final static String ALPINE_IMAGE = "natural/alpine-sshd:latest";
    private final static String CENTOS_IMAGE = "natural/centos-sshd:latest";
    private final static String DEBIAN_IMAGE = "natural/debian-sshd:latest";
    private final static String UBUNTU_IMAGE = "natural/ubuntu-sshd:latest";
    private final static List<String> TEST_CONTAINER_IMAGES =
        Arrays.asList(
                      ALPINE_IMAGE,
                      CENTOS_IMAGE,
                      DEBIAN_IMAGE,
                      UBUNTU_IMAGE
                      );

    private final static String CONTAINER_SSH_AUTH_KEYS = "/root/.ssh/authorized_keys";
    private final static Integer CONTAINER_SSH_PORT = 22;

    private static String sshPrivKeyFile;
    private static String sshPubKeyFile;

    private AuthorizerConfigurationContext authContext;
    private ShellUserGroupProvider localProvider;
    private UserGroupProviderInitializationContext initContext;

    @ClassRule
    static public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setupOnce() throws IOException {
        sshPrivKeyFile = tempFolder.getRoot().getAbsolutePath() + "/id_rsa";
        sshPubKeyFile = sshPrivKeyFile + ".pub";

        try {
            // NB: this command is a bit perplexing: it works without prompt from the shell, but hangs
            // here without the pipe from `yes`:
            ShellRunner.runShell("yes | ssh-keygen -C '' -N '' -t rsa -f " + sshPrivKeyFile);
        } catch (final IOException ioexc) {
            systemCheckFailed = true;
            logger.error("setupOnce() exception: " + ioexc + "; tests cannot run on this system.");
            return;
        }

        // Fix the file permissions to abide by the ssh client
        // requirements:
        Arrays.asList(sshPrivKeyFile, sshPubKeyFile).forEach(name -> {
                final File f = new File(name);
                Assert.assertTrue(f.setReadable(false, false));
                Assert.assertTrue(f.setReadable(true));
            });
    }

    @Before
    public void setup() throws IOException {
        authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        initContext = Mockito.mock(UserGroupProviderInitializationContext.class);

        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.INITIAL_REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("10"));
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("15"));

        localProvider = new ShellUserGroupProvider();
        try {
            localProvider.initialize(initContext);
            localProvider.onConfigured(authContext);
        } catch (final Exception exc) {
            systemCheckFailed = true;
            logger.error("setup() exception: " + exc + "; tests cannot run on this system.");
            return;
        }
        Assert.assertEquals(10, localProvider.getInitialRefreshDelay());
        Assert.assertEquals(15, localProvider.getRefreshDelay());
    }

    @After
    public void tearDown() {
        localProvider.preDestruction();
    }

    // Our primary test methods all accept a provider; here we define overloads to those methods to
    // use the local provider.  This allows the reuse of those test methods with the remote provider.

    @Test
    public void testGetUsersAndUsersMinimumCount() {
        testGetUsersAndUsersMinimumCount(localProvider);
    }

    @Test
    public void testGetKnownUserByUsername() {
        testGetKnownUserByUsername(localProvider);
    }

    @Test
    public void testGetKnownUserByUid() {
        testGetKnownUserByUid(localProvider);
    }

    @Test
    public void testGetGroupsAndMinimumGroupCount() {
        testGetGroupsAndMinimumGroupCount(localProvider);
    }

    @Test
    public void testGetKnownGroupByGid() {
        testGetKnownGroupByGid(localProvider);
    }

    @Test
    public void testGetGroupByGidAndGetGroupMembership() {
        testGetGroupByGidAndGetGroupMembership(localProvider);
    }

    @Test
    public void testGetUserByUidAndGetGroupMembership() {
        testGetUserByUidAndGetGroupMembership(localProvider);
    }

    @SuppressWarnings("RedundantThrows")
    private GenericContainer createContainer(String image) throws IOException, InterruptedException {
        GenericContainer container = new GenericContainer(image)
            .withEnv("SSH_ENABLE_ROOT", "true").withExposedPorts(CONTAINER_SSH_PORT);
        container.start();

        // This can go into the docker images:
        container.execInContainer("mkdir", "-p", "/root/.ssh");
        container.copyFileToContainer(MountableFile.forHostPath(sshPubKeyFile),  CONTAINER_SSH_AUTH_KEYS);
        return container;
    }

    private UserGroupProvider createRemoteProvider(GenericContainer container) {
        final ShellCommandsProvider remoteCommands =
            RemoteShellCommands.wrapOtherProvider(new NssShellCommands(),
                                                  sshPrivKeyFile,
                                                  container.getContainerIpAddress(),
                                                  container.getMappedPort(CONTAINER_SSH_PORT));

        ShellUserGroupProvider remoteProvider = new ShellUserGroupProvider();
        remoteProvider.setCommandsProvider(remoteCommands);
        remoteProvider.initialize(initContext);
        remoteProvider.onConfigured(authContext);
        return remoteProvider;
    }

    @Ignore
    public void someOtherTest() {}

    @Test
    public void testVariousSystemImages() {
        // Here we explicitly clear the system check flag to allow the remote checks that follow:
        systemCheckFailed = false;
        Assume.assumeTrue(isTestableEnvironment());
        // systemCheckFailed = true;

        TEST_CONTAINER_IMAGES.forEach(image -> {
                GenericContainer container;
                UserGroupProvider remoteProvider;
                logger.debug("creating container from image: " + image);

                try {
                    container = createContainer(image);
                } catch (final Exception exc) {
                    logger.error("create container exception: " + exc);
                    return;
                }
                try {
                    remoteProvider = createRemoteProvider(container);
                } catch (final Exception exc) {
                    logger.error("create user provider exception: " + exc);
                    return;
                }

                try {
                    testGetUsersAndUsersMinimumCount(remoteProvider);
                    testGetKnownUserByUsername(remoteProvider);
                    testGetGroupsAndMinimumGroupCount(remoteProvider);
                    testGetKnownGroupByGid(remoteProvider);
                    testGetGroupByGidAndGetGroupMembership(remoteProvider);
                    testGetUserByUidAndGetGroupMembership(remoteProvider);
                } catch (final Exception e) {
                    logger.error("Exception running remote provider on image: " + image +  ", exception: " + e);
                }

                container.stop();
                remoteProvider.preDestruction();
                logger.debug("finished with container image: " + image);
            });
    }
}
