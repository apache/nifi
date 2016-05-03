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

import java.io.Console;
import java.io.InvalidObjectException;
import java.nio.CharBuffer;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;


/**
 * <p>Command-line interface for generating password hashes compatible with
 * {@link CredentialsStore}.</p>
 *
 * <p>Usage: [algorithm] [options...]</p>
 * <ul style="list-style-type:none">
 *   <li>bcrypt &lt;rounds&gt;</li>
 * </ul>
 *
 * <p>Examples:</p>
 * <ul style="list-style-type:none">
 *   <li>bcrypt</li>
 *   <li>bcrypt 15</li>
 * </ul>
 *
 * <p>Requires spring-security-core, either in the classpath or by generating
 * the nifi-file-identity provider JAR with dependencies:
 * {@code mvn compile assembly:single}</p>
 *
 * @see CredentialsStore
 */
public class PasswordHasherCLI {

    public static void main(String[] args) {
        final PasswordHasherCLI cli = new PasswordHasherCLI();
        final HashAction action = cli.processArgs(args);
        try {
            action.promptForSecureInput();
            action.execute();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        for (String line : action.outputs) {
            System.out.println(line);
        }
    }

    HashAction processArgs(String[] args) {
        HashAction action = null;
        if (args.length < 1) {
            action = new PrintHelpAction();
            return action;
        }
        String command = args[0];
        switch (command) {
            case "bcrypt":
                action = new BcryptAction();
                break;
            default:
                action = new PrintHelpAction();
                break;
        }
        action.setArgs(args);
        return action;
    }

    abstract class HashAction {
        abstract void execute() throws Exception;

        String[] args = null;
        String[] outputs = new String[]{};
        String securePrompt = "Password: ";
        char[] secureInput = null;

        void setArgs(String args[]) {
            this.args = args;
        }

        void promptForSecureInput() throws InvalidObjectException {
            if (securePrompt == null) {
                return;
            }
            Console console = System.console();
            if (console == null) {
                throw new InvalidObjectException("Console is not available for reading password");
            }
            secureInput = console.readPassword(securePrompt);
        }

    }

    class PrintHelpAction extends HashAction {
        PrintHelpAction() {
            securePrompt = null;
        }
        void execute() {
            this.outputs = new String[]{
                    "Password Hasher",
                    "Usage: [ALGORITHM] [OPTIONS]...",
                    "  bcrypt <rounds=10>",
                    "Examples:",
                    "  bcrypt",
                    "  bcrypt 15"
            };
        }
    }

    class BcryptAction extends HashAction {
        void execute() throws Exception {
            int rounds = 10;
            if (args.length > 1) {
                String rawRounds = args[1];
                rounds = Integer.parseInt(rawRounds);
            }
            final PasswordEncoder passwordEncoder = new BCryptPasswordEncoder(rounds);

            CharBuffer buffer = CharBuffer.wrap(secureInput);
            String hashed = passwordEncoder.encode(buffer);
            java.util.Arrays.fill(secureInput, ' ');
            this.outputs = new String[]{hashed};
        }
    }

}

