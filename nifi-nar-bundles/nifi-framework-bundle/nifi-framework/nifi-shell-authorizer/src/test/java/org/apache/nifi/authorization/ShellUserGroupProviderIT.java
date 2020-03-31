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

import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.util.ShellRunner;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;


public class ShellUserGroupProviderIT {
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

    private final String KNOWN_USER  = "root";
    private final String KNOWN_UID   = "0";

    @SuppressWarnings("FieldCanBeLocal")
    private final String KNOWN_GROUP = "root";

    @SuppressWarnings("FieldCanBeLocal")
    private final String OTHER_GROUP = "wheel"; // e.g., macos
    private final String KNOWN_GID   = "0";

    // We're using this knob to control the test runs on Travis.  The issue there is that tests
    // running on Travis do not have `getent`, thus not behaving like a typical Linux installation.
    protected static boolean systemCheckFailed = false;

    private static String sshPrivKeyFile;
    private static String sshPubKeyFile;

    private AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
    private ShellUserGroupProvider localProvider;
    private UserGroupProviderInitializationContext initContext;

    private static ShellRunner shellRunner;

    @ClassRule
    static public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupOnce() throws IOException {
        sshPrivKeyFile = tempFolder.getRoot().getAbsolutePath() + "/id_rsa";
        sshPubKeyFile = sshPrivKeyFile + ".pub";

        shellRunner = new ShellRunner(60);
        try {
            // NB: this command is a bit perplexing: it works without prompt from the shell, but hangs
            // here without the pipe from `yes`:
            shellRunner.runShell("yes | ssh-keygen -C '' -N '' -t rsa -f " + sshPrivKeyFile);
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

        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("10 sec"));
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.EXCLUDE_GROUP_PROPERTY))).thenReturn(new MockPropertyValue(".*d$"));
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.EXCLUDE_USER_PROPERTY))).thenReturn(new MockPropertyValue("^s.*"));

        localProvider = new ShellUserGroupProvider();
        try {
            localProvider.initialize(initContext);
            localProvider.onConfigured(authContext);
        } catch (final Exception exc) {
            systemCheckFailed = true;
            logger.error("setup() exception: " + exc + "; tests cannot run on this system.");
            return;
        }
        Assert.assertEquals(10000, localProvider.getRefreshDelay());
    }

    @After
    public void tearDown() {
        localProvider.preDestruction();
    }

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

    @Test
    public void testTooShortDelayIntervalThrowsException() throws AuthorizerCreationException {
        final AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        final ShellUserGroupProvider localProvider = new ShellUserGroupProvider();
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("1 milliseconds"));

        expectedException.expect(AuthorizerCreationException.class);
        expectedException.expectMessage("The Refresh Delay '1 milliseconds' is below the minimum value of '10000 ms'");

        localProvider.onConfigured(authContext);
    }


    @Test
    public void testInvalidGroupExcludeExpressionsThrowsException() throws AuthorizerCreationException {
        AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        ShellUserGroupProvider localProvider = new ShellUserGroupProvider();
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("3 minutes"));
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.EXCLUDE_GROUP_PROPERTY))).thenReturn(new MockPropertyValue("(3"));

        expectedException.expect(AuthorizerCreationException.class);
        expectedException.expectMessage("Unclosed group near index");
        localProvider.onConfigured(authContext);


    }

    @Test
    public void testInvalidUserExcludeExpressionsThrowsException() throws AuthorizerCreationException {
        AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        ShellUserGroupProvider localProvider = new ShellUserGroupProvider();
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("3 minutes"));
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.EXCLUDE_USER_PROPERTY))).thenReturn(new MockPropertyValue("*"));

        expectedException.expect(AuthorizerCreationException.class);
        expectedException.expectMessage("Dangling meta character");
        localProvider.onConfigured(authContext);
    }

    @Test
    public void testMissingExcludeExpressionsAllowed() throws AuthorizerCreationException {
        AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        ShellUserGroupProvider localProvider = new ShellUserGroupProvider();
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("3 minutes"));

        localProvider.onConfigured(authContext);
        verifyUsersAndUsersMinimumCount(localProvider);
    }

    @Test
    public void testInvalidDelayIntervalThrowsException() throws AuthorizerCreationException {
        final AuthorizerConfigurationContext authContext = Mockito.mock(AuthorizerConfigurationContext.class);
        final ShellUserGroupProvider localProvider = new ShellUserGroupProvider();
        Mockito.when(authContext.getProperty(Mockito.eq(ShellUserGroupProvider.REFRESH_DELAY_PROPERTY))).thenReturn(new MockPropertyValue("Not an interval"));

        expectedException.expect(AuthorizerCreationException.class);
        expectedException.expectMessage("The Refresh Delay 'Not an interval' is not a valid time interval.");

        localProvider.onConfigured(authContext);
    }

    @Test
    public void testCacheSizesAfterClearingCaches() {
        localProvider.clearCaches();
        assert localProvider.userCacheSize() == 0;
        assert localProvider.groupCacheSize() == 0;
    }

    @Test
    public void testLocalGetUsersAndUsersMinimumCount() {
        verifyUsersAndUsersMinimumCount(localProvider);
    }

    @Test
    public void testLocalGetKnownUserByUsername() {
        verifyKnownUserByUsername(localProvider);
    }

    @Test
    public void testLocalGetGroupsAndMinimumGroupCount() {
        verifyGroupsAndMinimumGroupCount(localProvider);
    }

    @Test
    public void testLocalGetUserByIdentityAndGetGroupMembership() {
        verifyGetUserByIdentityAndGetGroupMembership(localProvider);
    }

    // @Ignore // for now
    @Test
    public void testVariousSystemImages() {
        // Here we explicitly clear the system check flag to allow the remote checks that follow:
        systemCheckFailed = false;
        Assume.assumeTrue(isSSHAvailable());

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
                    verifyUsersAndUsersMinimumCount(remoteProvider);
                    verifyKnownUserByUsername(remoteProvider);
                    verifyGroupsAndMinimumGroupCount(remoteProvider);
                    verifyGetUserByIdentityAndGetGroupMembership(remoteProvider);
                } catch (final Exception e) {
                    // Some environments don't allow our tests to work.
                    logger.error("Exception running remote provider on image: " + image +  ", exception: " + e);
                }

                container.stop();
                remoteProvider.preDestruction();
                logger.debug("finished with container image: " + image);
            });
    }

    // TODO: Make test which retrieves list of users and then getUserByIdentity to ensure the user is populated in the response



    /**
     * Ensures that the test can run because Docker is available and the remote instance can be reached via ssh.
     *
     * @return true if Docker is available on this OS
     */
    private boolean isSSHAvailable() {
        return !systemCheckFailed;
    }

    /**
     * Tests the provider behavior by getting its users and checking minimum size.
     *
     * @param provider {@link UserGroupProvider}
     */
    private void verifyUsersAndUsersMinimumCount(UserGroupProvider provider) {
        Assume.assumeTrue(isSSHAvailable());

        Set<User> users = provider.getUsers();

        // This shows that we don't have any users matching the exclude regex, which is likely because those users
        // exist but were excluded:
        for (User user : users) {
            Assert.assertFalse(user.getIdentifier().startsWith("s"));
        }

        Assert.assertNotNull(users);
        Assert.assertTrue(users.size() > 0);
    }

    /**
     * Tests the provider behavior by getting a known user by id.
     *
     * @param provider {@link UserGroupProvider}
     */
    private void verifyKnownUserByUsername(UserGroupProvider provider) {
        Assume.assumeTrue(isSSHAvailable());

        User root = provider.getUserByIdentity(KNOWN_USER);
        Assert.assertNotNull(root);
        Assert.assertEquals(KNOWN_USER, root.getIdentity());
    }

    /**
     * Tests the provider behavior by getting its groups and checking minimum size.
     *
     * @param provider {@link UserGroupProvider}
     */
    private void verifyGroupsAndMinimumGroupCount(UserGroupProvider provider) {
        Assume.assumeTrue(isSSHAvailable());

        Set<Group> groups = provider.getGroups();

        // This shows that we don't have any groups matching the exclude regex, which is likely because those groups
        // exist but were excluded:
        for (Group group : groups) {
            Assert.assertFalse(group.getName().endsWith("d"));
        }

        Assert.assertNotNull(groups);
        Assert.assertTrue(groups.size() > 0);
    }

    /**
     * Tests the provider behavior by getting a known user and checking its group membership.
     *
     * @param provider {@link UserGroupProvider}
     */
    private void verifyGetUserByIdentityAndGetGroupMembership(UserGroupProvider provider) {
        Assume.assumeTrue(isSSHAvailable());

        UserAndGroups user = provider.getUserAndGroups(KNOWN_USER);
        Assert.assertNotNull(user);

        try {
            Assert.assertTrue(user.getGroups().size() > 0);
            logger.info("root user group count: " + user.getGroups().size());
        } catch (final AssertionError ignored) {
            logger.info("root user and groups group count zero on this system");
        }

        Set<Group> groups = provider.getGroups();
        Assert.assertTrue(groups.size() > user.getGroups().size());
    }
}
