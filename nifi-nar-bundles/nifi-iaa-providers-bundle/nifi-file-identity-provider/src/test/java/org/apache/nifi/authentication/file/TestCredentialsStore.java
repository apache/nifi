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
import java.io.InvalidObjectException;
import java.util.List;

import org.apache.nifi.authentication.file.generated.UserCredentials;
import org.apache.nifi.authentication.file.generated.UserCredentialsList;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class TestCredentialsStore {

    private static final String TEST_CREDENTIALS_FILE = "src/test/resources/test_credentials.xml";
    private static final String TEST_INVALID_CREDENTIALS_FILE = "src/test/resources/test_credentials_invalid.xml";
    private static final String TEST_DUPLICATE_USER_CREDENTIALS_FILE = "src/test/resources/test_credentials_duplicate.xml";
    private static final String TEST_READ_WRITE_CREDENTIALS_FILE = "src/test/resources/test_read_write_credentials.xml";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test(expected = FileNotFoundException.class)
    public void testConfigFileNotFound() throws Exception {
        final UserCredentialsList credentials = CredentialsStore.loadCredentialsList("NonExistingFile.xml");
    }

    @Test
    public void testLoadCredentialsFile() throws Exception {
        final UserCredentialsList credentials = CredentialsStore.loadCredentialsList(TEST_CREDENTIALS_FILE);
        final List<UserCredentials> users = credentials.getUser();
        Assert.assertEquals(2, users.size());

        final UserCredentials userCred = users.get(0);
        Assert.assertEquals("user1", userCred.getName());
        Assert.assertEquals("fakePasswordHash", userCred.getPasswordHash());
    }

    @Test
    public void testLoadInvalidCredentialsFileMessaging() throws Exception {
        try {
            final UserCredentialsList credentials = CredentialsStore.loadCredentialsList(TEST_INVALID_CREDENTIALS_FILE);
        } catch (javax.xml.bind.UnmarshalException unmarshalEx) {
            String exceptionMessage = unmarshalEx.toString();
            Assert.assertTrue(exceptionMessage.contains("invalid_credentials"));
            Assert.assertTrue(exceptionMessage.contains(TEST_INVALID_CREDENTIALS_FILE));
        }
    }

    @Test
    public void testLoadInvalidDuplicateUserCredentialsFileMessaging() throws Exception {
        try {
            final UserCredentialsList credentials = CredentialsStore.loadCredentialsList(TEST_DUPLICATE_USER_CREDENTIALS_FILE);
            Assert.fail("Duplicate user in credentials file should throw an exception");
        } catch (javax.xml.bind.UnmarshalException unmarshalEx) {
            String exceptionMessage = unmarshalEx.toString();
            Assert.assertTrue(exceptionMessage.contains("unique"));
            Assert.assertTrue(exceptionMessage.contains(TEST_DUPLICATE_USER_CREDENTIALS_FILE));
        } catch (Exception ex) {
        }
    }

    @Test
    public void testReadWriteCredsFile() throws Exception {
        File tempFile = folder.newFile("testReadWriteCredsFile_actual.xml");
        final UserCredentialsList credentials = CredentialsStore.loadCredentialsList(TEST_READ_WRITE_CREDENTIALS_FILE);
        CredentialsStore.saveCredentialsList(credentials, tempFile);
        String actualContent = FileUtils.readFileToString(tempFile);
        File expectedFile = new File(TEST_READ_WRITE_CREDENTIALS_FILE);
        String expectedContent = FileUtils.readFileToString(expectedFile);
        Assert.assertEquals(expectedContent, actualContent);
    }

    @Test
    public void testNewCredsFile() throws Exception {
        CredentialsStore credStore = new CredentialsStore();
        final String userName = "Some User";
        credStore.addUser(userName, "SuperSecret");
        File tempFile = folder.newFile("testNewCredsFile_actual.xml");
        credStore.save(tempFile);
        CredentialsStore testStore = CredentialsStore.fromFile(tempFile);
        UserCredentialsList credentialsList = testStore.getCredentialsList();
        Assert.assertEquals(1, credentialsList.getUser().size());
        boolean passwordMatches = testStore.checkPassword(userName, "SuperSecret");
        Assert.assertTrue(passwordMatches);
    }

    @Test
    public void testResetPassword() throws Exception {
        CredentialsStore credStore = CredentialsStore.fromFile(TEST_READ_WRITE_CREDENTIALS_FILE);
        final String userName = "Some User";
        credStore.addUser(userName, "SuperSecret");
        boolean passwordMatches = credStore.checkPassword(userName, "SuperSecret");
        Assert.assertTrue(passwordMatches);
        credStore.resetPassword(userName, "SuperDuperSecret");
        passwordMatches = credStore.checkPassword(userName, "SuperDuperSecret");
        Assert.assertTrue(passwordMatches);
    }

    @Test
    public void testCheckPassword() throws Exception {
        CredentialsStore credStore = CredentialsStore.fromFile(TEST_READ_WRITE_CREDENTIALS_FILE);
        final String userName = "Some User";
        credStore.addUser(userName, "SuperSecret");
        boolean passwordMatches = credStore.checkPassword(userName, "SuperSecret");
        Assert.assertTrue(passwordMatches);
        passwordMatches = credStore.checkPassword(userName, "WrongPassword");
        Assert.assertFalse(passwordMatches);
    }

    @Test
    public void testRemoveUser() throws Exception {
        CredentialsStore credStore = CredentialsStore.fromFile(TEST_READ_WRITE_CREDENTIALS_FILE);
        final String userName = "Some User";
        credStore.addUser(userName, "SuperSecret");
        boolean removed = credStore.removeUser(userName);
        Assert.assertTrue(removed);
        UserCredentials userCreds = credStore.findUser(userName);
        Assert.assertNull(userCreds);
        removed = credStore.removeUser(userName);
        Assert.assertFalse(removed);
    }

    @Test
    public void testCredentialsStoreReloadsFileUpdates() throws Exception {
        File tempFile = folder.newFile("testCredentialsStoreReloadsFileUpdates_actual.xml");
        CredentialsStore credStore = new CredentialsStore(tempFile);
        final String userName = "Some User";
        final String password1 = "SuperSecret1";
        credStore.addUser(userName, password1);
        credStore.save();
        boolean reloaded = credStore.reloadIfModified();
        Assert.assertFalse(reloaded);
        CredentialsStore testStore1 = CredentialsStore.fromFile(tempFile);
        CredentialsStore testStore2 = CredentialsStore.fromFile(tempFile);
        final String password2 = "SuperSecret2";
        testStore1.resetPassword(userName, password2);
        testStore1.save();
        // Ensure significant last modified diff on low-granularity file systems
        long lastModified = tempFile.lastModified();
        tempFile.setLastModified(lastModified + 5000);
        reloaded = testStore2.reloadIfModified();
        Assert.assertTrue(reloaded);
        boolean passwordMatches = testStore2.checkPassword(userName, password1);
        Assert.assertFalse(passwordMatches);
        passwordMatches = testStore2.checkPassword(userName, password2);
        Assert.assertTrue(passwordMatches);
    }

    @Test(expected = InvalidObjectException.class)
    public void testSaveWithoutFileThrows() throws Exception {
        CredentialsStore credStore = new CredentialsStore();
        final String userName = "Some User";
        final String password1 = "SuperSecret1";
        credStore.addUser(userName, password1);
        credStore.save();
    }

}
