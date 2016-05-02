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
package org.apache.nifi.authentication.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.file.CredentialsCLI.CredentialsAction;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class TestCredentialsCLI {

    private static final String TEST_CLI_CREDENTIALS_FILE = "src/test/resources/test_cli_credentials.xml";
    private String credentialsFilePath;
    private String tempFolderPath;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setupTestCredentialsFile() throws Exception {
        final File tempFolder = folder.newFolder();
        tempFolderPath = tempFolder.getAbsolutePath();
        credentialsFilePath = Paths.get(tempFolderPath, "credentials.xml").toString();
        final File resourceFile = new File(TEST_CLI_CREDENTIALS_FILE);
        final File tempFile = new File(credentialsFilePath);
        FileUtils.copyFile(resourceFile, tempFile);
    }

    @Test
    public void testHelp() throws Exception {
        final String[] args = new String[]{};
        final CredentialsCLI cli = new CredentialsCLI();
        final CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.PrintHelpAction.class, action.getClass());
        action.Execute();
        final String helpText = StringUtils.join(action.outputs, "\n");
        Assert.assertTrue(helpText.contains("Usage"));
    }

    @Test
    public void testUnknownCommandsGetHelp() {
        String[] args = new String[]{"bogus"};
        final CredentialsCLI cli = new CredentialsCLI();
        CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.PrintHelpAction.class, action.getClass());
        args = new String[]{"bogus", "user", "password"};
        action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.PrintHelpAction.class, action.getClass());
    }

    @Test
    public void testListWithoutFilePrintsError() {
        final String[] args = new String[]{"list"};
        final CredentialsCLI cli = new CredentialsCLI();
        final CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.PrintHelpAction.class, action.getClass());
    }

    @Test(expected = FileNotFoundException.class)
    public void testListWithBadFileThrows() throws Exception {
        final String[] args = new String[]{"list", "NoSuchFile.xml"};
        final CredentialsCLI cli = new CredentialsCLI();
        final CredentialsAction action = cli.processArgs(args);
        action.Execute();
    }

    @Test
    public void testListUsersSimple() throws Exception {
        final String[] args = new String[]{"list", credentialsFilePath};
        final CredentialsCLI cli = new CredentialsCLI();
        final CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.ListUsersAction.class, action.getClass());
        action.Execute();
        Assert.assertEquals(action.outputs.length, 6);
        Assert.assertEquals(action.outputs[0], "user1");
    }

    @Test
    public void testAddUserCreatesFile() throws Exception {
        final String credentialsFilePath = Paths.get(tempFolderPath, "testAddUserCreatesFile.xml").toString();
        final String[] args = new String[]{"add", credentialsFilePath, "someuser", "password1"};
        final CredentialsCLI cli = new CredentialsCLI();
        final CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.AddUserAction.class, action.getClass());
        action.Execute();
        Assert.assertEquals(action.outputs.length, 1);
        Assert.assertTrue(action.outputs[0].contains("someuser"));
    }

    @Test
    public void testResetPassword() throws Exception {
        final String userName = "user1";
        final String[] args = new String[]{"reset", credentialsFilePath, "user1", "ResetPassword"};
        final CredentialsCLI cli = new CredentialsCLI();
        final CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.ResetPasswordAction.class, action.getClass());
        action.Execute();
        Assert.assertEquals(action.outputs.length, 1);
        Assert.assertTrue(action.outputs[0].contains("user1"));
        final CredentialsStore credStore = CredentialsStore.fromFile(credentialsFilePath);
        boolean passwordMatches = credStore.checkPassword(userName, "ResetPassword");
        Assert.assertTrue(passwordMatches);
    }

    @Test
    public void testRemoveUser() throws Exception {
        final String userName = "user1";
        final String[] args = new String[]{"remove", credentialsFilePath, userName};
        final CredentialsCLI cli = new CredentialsCLI();
        CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.RemoveUserAction.class, action.getClass());
        action.Execute();
        Assert.assertEquals(action.outputs.length, 1);
        Assert.assertTrue(action.outputs[0].contains(userName));
        action = cli.processArgs(new String[] {"list", credentialsFilePath});
        action.Execute();
        Assert.assertEquals(5, action.outputs.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveUserNoParameter() throws Exception {
        final String[] args = new String[]{"remove", credentialsFilePath};
        final CredentialsCLI cli = new CredentialsCLI();
        CredentialsAction action = cli.processArgs(args);
        Assert.assertEquals(CredentialsCLI.RemoveUserAction.class, action.getClass());
        action.Execute();
    }

}
