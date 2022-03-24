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
package org.apache.nifi.toolkit.cli.impl.command.registry;

import org.apache.nifi.toolkit.cli.CLIMain;
import org.apache.nifi.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RegistryManualIT {
    public static final String TRUSTSTORE = "";
    public static final String TRUSTSTORE_PASSWD = "";

    public static final String KEYSTORE = "";
    public static final String KEYSTORE_PASSWD = "";

    private String basicSettings = "--baseUrl https://localhost:18443 --verbose";
    private String securitySettings = "--truststore " + TRUSTSTORE +
        " --truststoreType jks --truststorePasswd " + TRUSTSTORE_PASSWD +
        " --keystore " + KEYSTORE +
        " --keystorePasswd " + KEYSTORE_PASSWD +
        " --keystoreType PKCS12";

    private static final String TEST_USER_NAME = "testUser";
    private static final String TEST_USER_GROUP_NAME = "testUserGroup";
    private String testUserId;
    private String testUserGroupId;
    private static final String TEST_BUCKET_NAME = "testBucket";
    private String testBucketId;
    private PrintStream originalStdOut;
    private ByteArrayOutputStream out;

    @Before
    public void setUp() throws Exception {
        assertFalse("truststore not set", StringUtils.isBlank(TRUSTSTORE));
        assertFalse("truststorePasswd not set", StringUtils.isBlank(TRUSTSTORE_PASSWD));
        assertFalse("keystore not set", StringUtils.isBlank(KEYSTORE));
        assertFalse("keystorePassed not set", StringUtils.isBlank(KEYSTORE_PASSWD));
    }

    @After
    public void tearDown() throws Exception {
        resetStdOut();
    }

    @Ignore("Run first and only once")
    @Test
    public void testCreateUser() throws Exception {
        String userName = TEST_USER_NAME;

        runRegistryCommand("create-user",
            "--userName " + userName
        );

        testListUsers(userName);
    }

    @Ignore("Run first and only once")
    @Test
    public void testCreateUseGroup() throws Exception {
        String expectedUserGroup = TEST_USER_GROUP_NAME;

        runRegistryCommand("create-user-group",
            "--userGroupName " + expectedUserGroup +
                " --userNameList " + TEST_USER_NAME +
                " --userIdList " + testUserId
        );

       testListUserGroup(expectedUserGroup);
    }

    @Ignore("Run first and only once")
    @Test
    public void testCreateBucket() throws Exception {
        runRegistryCommand("create-bucket", "--bucketName " + TEST_BUCKET_NAME);
        testListBuckets(TEST_BUCKET_NAME);
    }

    @Test
    public void testListUsers() throws Exception {
        testListUsers(TEST_USER_NAME);
    }

    @Test
    public void testUpdateUser() throws Exception {
        String originalUserName = TEST_USER_NAME;
        String updatedUserName = "updatedTestUser";

        testListUsers(originalUserName);

        runRegistryCommand("update-user",
            "--userName " + updatedUserName +
                " --userId " + testUserId
        );

        testListUsers(updatedUserName);

        runRegistryCommand("update-user",
            "--userName " + originalUserName +
                " --userId " + testUserId
        );

        testListUsers(originalUserName);
    }

    @Test
    public void testListUserGroups() throws Exception {
        testListUserGroup(TEST_USER_GROUP_NAME);
    }

    @Test
    public void testUpdateUserGroup() throws Exception {
        runRegistryCommand("update-user-group",
            "--userGroupName " + TEST_USER_GROUP_NAME +
                " --userNameList " + TEST_USER_NAME +
                " --userIdLis " + testUserId +
                " --userGroupId " + testUserGroupId
        );

        testListUserGroup(TEST_USER_GROUP_NAME);
    }

    @Test
    public void testGetAccessPolicy() throws Exception {
        String action = "read";
        String resource = "/testResource";

        testGetAccessPolicy(action, resource);
    }

    @Test
    public void testCreateOrUpdateAccessPolicy() throws Exception {
        String action = "read";
        String resource = "/testResource";

        runRegistryCommand("update-policy",
            "--accessPolicyResource " + resource +
                " --accessPolicyAction " + action +
                " --userNameList " + TEST_USER_NAME +
                " --userIdList " + testUserId +
                " --groupNameList " + TEST_USER_GROUP_NAME +
                " --groupIdList " + testUserGroupId
        );

        testGetAccessPolicy(action, resource);

        // Users are not removed if none is specified
        runRegistryCommand("update-policy",
            "--accessPolicyResource " + resource +
                " --accessPolicyAction " + action +
                " --groupNameList " + TEST_USER_GROUP_NAME +
                " --groupIdList " + testUserGroupId
        );

        testGetAccessPolicy(action, resource);

        // Groups are not removed if none is specified
        runRegistryCommand("update-policy",
            "--accessPolicyResource " + resource +
                " --accessPolicyAction " + action +
                " --userNameList " + TEST_USER_NAME +
                " --userIdList " + testUserId
        );

        testGetAccessPolicy(action, resource);
    }

    @Test
    public void testUpdateBucketPolicyByName() throws Exception {
        String action = "/read";
        runRegistryCommand("update-bucket-policy",
                "--bucketName " + TEST_BUCKET_NAME +
                " --accessPolicyAction " + action +
                " --userNameList " + TEST_USER_NAME +
                " --userIdList " + testUserId
        );

        testGetAccessPolicy(action, testBucketId);
    }

    @Test public void testUpdateBucketPolicyById() throws Exception {
        String action = "/write";
        runRegistryCommand("update-bucket-policy",
                "--bucketId " + testBucketId +
                " --accessPolicyAction " + action +
                " --groupNameList " + TEST_USER_GROUP_NAME +
                " --groupIdList " + testUserGroupId
        );

        testGetAccessPolicy(action, testBucketId);
    }

    @Test
    public void testListBuckets() throws Exception {
        testListBuckets(TEST_BUCKET_NAME);
    }

    private void testListUsers(String expectedUserName) throws IOException {
        runCommand(
            "\\s{3,}",
            () -> runRegistryCommand("list-users", ""),
            words -> {
                if (words.length > 2 && words[1].equals(expectedUserName)) {
                    testUserId = words[2];
                }
            },
            () -> {
                assertNotNull(testUserId);
                assertTrue("User id shouldn't be blank!", !StringUtils.isBlank(testUserId));
            }
        );
    }

    private void testListUserGroup(String expectedUserGroupName) throws IOException {
        runCommand(
            "\\s{3,}",
            () -> runRegistryCommand("list-user-groups", ""),
            words -> {
                if (words.length > 3 && words[1].equals(expectedUserGroupName)) {
                    testUserGroupId = words[2];
                }
            },
            () -> {
                assertNotNull(testUserGroupId);
                assertTrue("Group id shouldn't be blank!", !StringUtils.isBlank(testUserGroupId));
            }
        );
    }

    private void testListBuckets(String expectedBucketName) throws IOException {
        runCommand("\\s{3,}",
                () -> runRegistryCommand("list-buckets",""),
                words -> {
                    if (words.length > 2 && words[1].equals(expectedBucketName)) {
                        testBucketId = words[2];
                    }
                },
                () -> {
                    assertNotNull(testBucketId);
                    assertTrue("Bucket ID should not be blank!", !StringUtils.isBlank(testBucketId));
                });
    }

    private void testGetAccessPolicy(String action, String resource) throws IOException {
        AtomicReference<String> resourceR = new AtomicReference<>();
        AtomicReference<String> actionR = new AtomicReference<>();
        AtomicReference<String> usersR = new AtomicReference<>();
        AtomicReference<String> groupsR = new AtomicReference<>();

        runCommand(
            "\\s*:\\s+",
            () -> runRegistryCommand("get-policy",
                "--accessPolicyResource " + resource +
                    " --accessPolicyAction " + action
            ),
            words -> {
                if (words.length > 1) {
                    if (words[0].equals("Users")) {
                        usersR.set(words[1]);
                    } else if (words[0].equals("Groups")) {
                        groupsR.set(words[1]);
                    } else if (words[0].equals("Resource")) {
                        resourceR.set(words[1]);
                    } else if (words[0].equals("Action")) {
                        actionR.set(words[1]);
                    }
                }
            },
            () -> {
                resourceR.get().equals(resource);
                actionR.get().contains(action);
                usersR.get().contains(TEST_USER_NAME);
                groupsR.get().contains(TEST_USER_GROUP_NAME);
            }
        );
    }

    private void runCommand(String delimiter, Runnable commandRunner, Consumer<String[]> outputLineHandler, Runnable check) throws IOException {
        try {
            // GIVEN
            startCaptureStdOut();

            // WHEN
            commandRunner.run();

            // THEN
            BufferedReader bufferedOutput = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(out.toByteArray())));

            String line;
            while ((line = bufferedOutput.readLine()) != null) {
                originalStdOut.println(line);

                String[] words = line.split(delimiter);

                outputLineHandler.accept(words);
            }

            check.run();
        } finally {
            resetStdOut();
        }
    }

    private void runRegistryCommand(String command, String commandArguments) {
        // GIVEN
        String arguments = new StringJoiner(" ")
            .add("registry")
            .add(command)
            .add(commandArguments)
            .add(basicSettings)
            .add(securitySettings)
            .toString();

        String[] args = arguments.split("\\s+");

        // WHEN
        CLIMain.runSingleCommand(args);

        // THEN
    }

    private void startCaptureStdOut() {
        originalStdOut = System.out;
        out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));
    }

    private void resetStdOut() {
        if (originalStdOut != null) {
            System.setOut(originalStdOut);
        }
    }
}
