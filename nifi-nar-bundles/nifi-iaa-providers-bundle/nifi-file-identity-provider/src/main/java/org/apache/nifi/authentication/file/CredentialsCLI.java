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
import java.util.ArrayList;

import org.apache.nifi.authentication.file.generated.UserCredentials;
import org.apache.nifi.authentication.file.generated.UserCredentialsList;


/**
 * <p>Command-line interface for working with a {@link CredentialsStore}
 * persisted as an XML file.</p>
 *
 * <p>Usage:</p>
 * <ul style="list-style-type:none">
 *   <li>list credentials.xml</li>
 *   <li>add credentials.xml admin password</li>
 *   <li>reset credentials.xml admin newpassword</li>
 *   <li>remove credentials.xml admin</li>
 * </ul>
 *
 * <p>Requires spring-security-core, either in the classpath or by generating
 * the nifi-file-identity provider JAR with dependencies:
 * {@code mvn compile assembly:single}</p>
 *
 * @see CredentialsStore
 */
public class CredentialsCLI {

    public static void main(String[] args) {
        final CredentialsCLI cli = new CredentialsCLI();
        final CredentialsAction action = cli.processArgs(args);
        try {
            action.Execute();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        for (String line : action.outputs) {
            System.out.println(line);
        }
    }

    CredentialsAction processArgs(String[] args) {
        CredentialsAction action = null;
        if (args.length < 2) {
            action = new PrintHelpAction();
            return action;
        }
        String command = args[0];
        String credentialsFile = args[1];
        switch (command) {
            case "list":
                action = new ListUsersAction(credentialsFile);
                break;
            case "add":
                action = new AddUserAction(credentialsFile);
                break;
            case "reset":
                action = new ResetPasswordAction(credentialsFile);
                break;
            case "remove":
                action = new RemoveUserAction(credentialsFile);
                break;
            default:
                action = new PrintHelpAction();
                break;
        }
        action.setArgs(args);
        return action;
    }

    abstract class CredentialsAction {
        abstract void Execute() throws Exception;
        String[] args = null;
        String[] outputs = new String[]{};
        String credentialsFilePath = null;

        CredentialsAction() {
        }

        CredentialsAction(String credentialsFilePath) {
            this.credentialsFilePath = credentialsFilePath;
        }

        CredentialsStore getCredentialsStore() throws Exception {
            File credentialsFile = new File(credentialsFilePath);
            CredentialsStore credStore;
            if (credentialsFile.exists()) {
                credStore = CredentialsStore.fromFile(credentialsFilePath);
            } else {
                credStore = new CredentialsStore();
            }
            return credStore;
        }

        void setArgs(String args[]) {
            this.args = args;
        }

        void assertCredentialsFileExists() throws FileNotFoundException {
            File credentialsFile = new File(credentialsFilePath);
            if (!credentialsFile.exists()) {
                throw new FileNotFoundException("The credentials file '" + credentialsFile + "' was not found");
            }
        }

        void assertArgsLength(int requiredLength) throws IllegalArgumentException {
            if (args == null || args.length < requiredLength) {
                throw new IllegalArgumentException(requiredLength + " arguments are required for this command");
            }
        }
    }

    class PrintHelpAction extends CredentialsAction {
        void Execute() {
            this.outputs = new String[] {
                    "Credentials Manager",
                    "Usage: [OPTION] [FILE]...",
                    "",
                    "Examples:",
                    "  list credentials.xml",
                    "  add credentials.xml admin password",
                    "  reset credentials.xml admin newpassword",
                    "  remove credentials.xml admin"
            };
        }
    }

    class ListUsersAction extends CredentialsAction {
        ListUsersAction(String credentialsFile) {
            super(credentialsFile);
        }
        void Execute() throws Exception {
            assertCredentialsFileExists();
            CredentialsStore credStore = getCredentialsStore();
            UserCredentialsList credentialsList = credStore.getCredentialsList();
            ArrayList<String> userOutputs = new ArrayList<String>();
            for (UserCredentials userCredentials : credentialsList.getUser()) {
                userOutputs.add(userCredentials.getName());
            }
            this.outputs = userOutputs.toArray(new String[]{});
        }
    }

    class AddUserAction extends CredentialsAction {
        AddUserAction(String credentialsFile) {
            super(credentialsFile);
        }
        void Execute() throws Exception {
            assertArgsLength(4);
            String userName = args[2];
            String rawPassword = args[3];
            CredentialsStore credStore = getCredentialsStore();
            credStore.addUser(userName, rawPassword);
            credStore.save(credentialsFilePath);
            this.outputs = new String[] {"Added user " + userName};
        }
    }

    class ResetPasswordAction extends CredentialsAction {
        ResetPasswordAction(String credentialsFile) {
            super(credentialsFile);
        }
        void Execute() throws Exception {
            assertArgsLength(4);
            String userName = args[2];
            String rawPassword = args[3];
            CredentialsStore credStore = getCredentialsStore();
            credStore.resetPassword(userName, rawPassword);
            credStore.save(credentialsFilePath);
            this.outputs = new String[] {"Password reset for user " + userName};
        }
    }

    class RemoveUserAction extends CredentialsAction {
        RemoveUserAction(String credentialsFile) {
            super(credentialsFile);
        }
        void Execute() throws Exception {
            assertArgsLength(3);
            assertCredentialsFileExists();
            String userName = args[2];
            CredentialsStore credStore = getCredentialsStore();
            credStore.removeUser(userName);
            credStore.save(credentialsFilePath);
            this.outputs = new String[] {"Removed user " + userName};
        }
    }

}
