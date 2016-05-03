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

import org.apache.nifi.authentication.file.PasswordHasherCLI.HashAction;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;


public class TestPasswordHasherCLI {

    @Test
    public void testHelp() throws Exception {
        final PasswordHasherCLI cli = new PasswordHasherCLI();
        final HashAction action = cli.processArgs(new String[]{});
        Assert.assertEquals(PasswordHasherCLI.PrintHelpAction.class, action.getClass());
        action.promptForSecureInput();
        action.execute();
        final String helpText = StringUtils.join(action.outputs, "\n");
        Assert.assertTrue(helpText.contains("Usage"));
    }

    @Test
    public void testUnknownCommandsGetHelp() {
        String[] args = new String[]{"bogus"};
        final PasswordHasherCLI cli = new PasswordHasherCLI();
        HashAction action = cli.processArgs(args);
        Assert.assertEquals(PasswordHasherCLI.PrintHelpAction.class, action.getClass());
    }

    @Test
    public void testBcryptDefaults() throws Exception {
        final String[] args = new String[]{"bcrypt"};
        final PasswordHasherCLI cli = new PasswordHasherCLI();
        final HashAction action = cli.processArgs(args);
        Assert.assertEquals(PasswordHasherCLI.BcryptAction.class, action.getClass());
        char[] secureChars = "SuperSecurePassword".toCharArray();
        action.secureInput = secureChars;
        action.execute();
        Assert.assertEquals(1, action.outputs.length);
        String hashedPassword = action.outputs[0];
        String hashPrefix = hashedPassword.substring(0, 7);
        Assert.assertEquals("$2a$10$", hashPrefix);
        String overwrittenSecureChars = String.valueOf(secureChars);
        Assert.assertTrue(overwrittenSecureChars.trim().isEmpty());
    }

    @Test
    public void testBcryptOptionRounds() throws Exception {
        final String[] args = new String[]{"bcrypt", "11"};
        final PasswordHasherCLI cli = new PasswordHasherCLI();
        final HashAction action = cli.processArgs(args);
        Assert.assertEquals(PasswordHasherCLI.BcryptAction.class, action.getClass());
        char[] secureChars = "SuperSecurePassword".toCharArray();
        action.secureInput = secureChars;
        action.execute();
        Assert.assertEquals(1, action.outputs.length);
        String hashedPassword = action.outputs[0];
        String hashPrefix = hashedPassword.substring(0, 7);
        Assert.assertEquals("$2a$11$", hashPrefix);
    }


}
