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

package org.apache.nifi.toolkit.tls.standalone;

import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriter;
import org.apache.nifi.toolkit.tls.util.PasswordUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.stubbing.defaultanswers.ForwardsInvocations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TlsToolkitStandaloneCommandLineTest {
    private SecureRandom secureRandom;
    private TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine;

    @Before
    public void setup() {
        secureRandom = mock(SecureRandom.class);
        doAnswer(new ForwardsInvocations(new Random())).when(secureRandom).nextBytes(any(byte[].class));
        tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine(new PasswordUtil(secureRandom));
    }

    @Test
    public void testHelp() {
        try {
            tlsToolkitStandaloneCommandLine.parse("-h");
            fail("Expected usage and help exit");
        } catch (CommandLineParseException e) {
            Assert.assertEquals(ExitCode.HELP.ordinal(), e.getExitCode());
        }
    }

    @Test
    public void testUnknownArg() {
        try {
            tlsToolkitStandaloneCommandLine.parse("--unknownArg");
            fail("Expected error parsing command line");
        } catch (CommandLineParseException e) {
            assertEquals(ExitCode.ERROR_PARSING_COMMAND_LINE.ordinal(), e.getExitCode());
        }
    }

    @Test
    public void testKeyAlgorithm() throws CommandLineParseException, IOException {
        String testKeyAlgorithm = "testKeyAlgorithm";
        tlsToolkitStandaloneCommandLine.parse("-a", testKeyAlgorithm);
        assertEquals(testKeyAlgorithm, tlsToolkitStandaloneCommandLine.createConfig().getKeyPairAlgorithm());
    }

    @Test
    public void testKeySizeArgNotInteger() {
        try {
            tlsToolkitStandaloneCommandLine.parse("-k", "badVal");
            fail("Expected bad keysize exit code");
        } catch (CommandLineParseException e) {
            assertEquals(ExitCode.ERROR_PARSING_INT_ARG.ordinal(), e.getExitCode());
        }
    }

    @Test
    public void testKeySize() throws CommandLineParseException, IOException {
        int testKeySize = 4096;
        tlsToolkitStandaloneCommandLine.parse("-k", Integer.toString(testKeySize));
        assertEquals(testKeySize, tlsToolkitStandaloneCommandLine.createConfig().getKeySize());
    }

    @Test
    public void testSigningAlgorithm() throws CommandLineParseException, IOException {
        String testSigningAlgorithm = "testSigningAlgorithm";
        tlsToolkitStandaloneCommandLine.parse("-s", testSigningAlgorithm);
        assertEquals(testSigningAlgorithm, tlsToolkitStandaloneCommandLine.createConfig().getSigningAlgorithm());
    }

    @Test
    public void testDaysNotInteger() {
        try {
            tlsToolkitStandaloneCommandLine.parse("-d", "badVal");
        } catch (CommandLineParseException e) {
            assertEquals(ExitCode.ERROR_PARSING_INT_ARG.ordinal(), e.getExitCode());
        }
    }

    @Test
    public void testDays() throws CommandLineParseException, IOException {
        int testDays = 29;
        tlsToolkitStandaloneCommandLine.parse("-d", Integer.toString(testDays));
        assertEquals(testDays, tlsToolkitStandaloneCommandLine.createConfig().getDays());
    }

    @Test
    public void testKeyStoreType() throws CommandLineParseException {
        String testKeyStoreType = "testKeyStoreType";
        tlsToolkitStandaloneCommandLine.parse("-T", testKeyStoreType);
        assertEquals(testKeyStoreType, tlsToolkitStandaloneCommandLine.getKeyStoreType());
    }

    @Test
    public void testOutputDirectory() throws CommandLineParseException {
        String testPath = File.separator + "fake" + File.separator + "path" + File.separator + "doesnt" + File.separator + "exist";
        tlsToolkitStandaloneCommandLine.parse("-o", testPath);
        assertEquals(testPath, tlsToolkitStandaloneCommandLine.getBaseDir().getPath());
    }

    @Test
    public void testHostnames() throws CommandLineParseException {
        String nifi1 = "nifi1";
        String nifi2 = "nifi2";

        tlsToolkitStandaloneCommandLine.parse("-n", nifi1 + " , " + nifi2);

        List<String> hostnames = tlsToolkitStandaloneCommandLine.getHostnames();
        assertEquals(2, hostnames.size());
        assertEquals(nifi1, hostnames.get(0));
        assertEquals(nifi2, hostnames.get(1));
    }

    @Test
    public void testHttpsPort() throws CommandLineParseException {
        int testPort = 8998;
        tlsToolkitStandaloneCommandLine.parse("-p", Integer.toString(testPort));
        assertEquals(testPort, tlsToolkitStandaloneCommandLine.getHttpsPort());
    }

    @Test
    public void testNifiPropertiesFile() throws CommandLineParseException, IOException {
        tlsToolkitStandaloneCommandLine.parse("-f", TlsToolkitStandaloneTest.TEST_NIFI_PROPERTIES);
        assertEquals(TlsToolkitStandaloneTest.FAKE_VALUE, getProperties().get(TlsToolkitStandaloneTest.NIFI_FAKE_PROPERTY));
    }

    @Test
    public void testNifiPropertiesFileDefault() throws CommandLineParseException, IOException {
        tlsToolkitStandaloneCommandLine.parse();
        assertNull(getProperties().get(TlsToolkitStandaloneTest.NIFI_FAKE_PROPERTY));
    }

    @Test
    public void testBadNifiPropertiesFile() {
        try {
            tlsToolkitStandaloneCommandLine.parse("-f", "/this/file/should/not/exist.txt");
            fail("Expected error when unable to read file");
        } catch (CommandLineParseException e) {
            assertEquals(ExitCode.ERROR_READING_NIFI_PROPERTIES.ordinal(), e.getExitCode());
        }
    }

    @Test
    public void testNotSameKeyAndKeystorePassword() throws CommandLineParseException {
        tlsToolkitStandaloneCommandLine.parse("-g");
        List<String> keyStorePasswords = tlsToolkitStandaloneCommandLine.getKeyStorePasswords();
        List<String> keyPasswords = tlsToolkitStandaloneCommandLine.getKeyPasswords();
        assertEquals(1, tlsToolkitStandaloneCommandLine.getHostnames().size());
        assertEquals(1, keyStorePasswords.size());
        assertEquals(1, keyPasswords.size());
        assertNotEquals(keyStorePasswords.get(0), keyPasswords.get(0));
    }

    @Test
    public void testSameKeyAndKeystorePassword() throws CommandLineParseException {
        tlsToolkitStandaloneCommandLine.parse();
        List<String> keyStorePasswords = tlsToolkitStandaloneCommandLine.getKeyStorePasswords();
        List<String> keyPasswords = tlsToolkitStandaloneCommandLine.getKeyPasswords();
        assertEquals(1, tlsToolkitStandaloneCommandLine.getHostnames().size());
        assertEquals(1, keyStorePasswords.size());
        assertEquals(1, keyPasswords.size());
        assertEquals(keyStorePasswords.get(0), keyPasswords.get(0));
    }

    @Test
    public void testSameKeyAndKeystorePasswordWithKeystorePasswordSpecified() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitStandaloneCommandLine.parse("-S", testPassword);
        List<String> keyStorePasswords = tlsToolkitStandaloneCommandLine.getKeyStorePasswords();
        assertEquals(1, keyStorePasswords.size());
        assertEquals(testPassword, keyStorePasswords.get(0));
        assertEquals(keyStorePasswords, tlsToolkitStandaloneCommandLine.getKeyPasswords());
    }

    @Test
    public void testSameKeyAndKeystorePasswordWithKeyPasswordSpecified() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitStandaloneCommandLine.parse("-K", testPassword);
        List<String> keyPasswords = tlsToolkitStandaloneCommandLine.getKeyPasswords();
        assertNotEquals(tlsToolkitStandaloneCommandLine.getKeyStorePasswords(), keyPasswords);
        assertEquals(1, keyPasswords.size());
        assertEquals(testPassword, keyPasswords.get(0));
    }

    @Test
    public void testKeyStorePasswordArg() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitStandaloneCommandLine.parse("-S", testPassword);
        List<String> keyStorePasswords = tlsToolkitStandaloneCommandLine.getKeyStorePasswords();
        assertEquals(1, keyStorePasswords.size());
        assertEquals(testPassword, keyStorePasswords.get(0));
    }

    @Test
    public void testMultipleKeystorePasswordArgs() throws CommandLineParseException {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        tlsToolkitStandaloneCommandLine.parse("-n", "nifi1,nifi2", "-S", testPassword1, "-S", testPassword2);
        List<String> keyStorePasswords = tlsToolkitStandaloneCommandLine.getKeyStorePasswords();
        assertEquals(2, keyStorePasswords.size());
        assertEquals(testPassword1, keyStorePasswords.get(0));
        assertEquals(testPassword2, keyStorePasswords.get(1));
    }

    @Test
    public void testMultipleKeystorePasswordArgSingleHost() {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        try {
            tlsToolkitStandaloneCommandLine.parse("-S", testPassword1, "-S", testPassword2);
            fail("Expected error with mismatch keystore password number");
        } catch (CommandLineParseException e) {
            assertEquals(ExitCode.ERROR_INCORRECT_NUMBER_OF_PASSWORDS.ordinal(), e.getExitCode());
        }
    }

    @Test
    public void testKeyPasswordArg() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitStandaloneCommandLine.parse("-K", testPassword);
        List<String> keyPasswords = tlsToolkitStandaloneCommandLine.getKeyPasswords();
        assertEquals(1, keyPasswords.size());
        assertEquals(testPassword, keyPasswords.get(0));
    }

    @Test
    public void testMultipleKeyPasswordArgs() throws CommandLineParseException {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        tlsToolkitStandaloneCommandLine.parse("-n", "nifi1,nifi2", "-K", testPassword1, "-K", testPassword2);
        List<String> keyPasswords = tlsToolkitStandaloneCommandLine.getKeyPasswords();
        assertEquals(2, keyPasswords.size());
        assertEquals(testPassword1, keyPasswords.get(0));
        assertEquals(testPassword2, keyPasswords.get(1));
    }

    @Test
    public void testMultipleKeyPasswordArgSingleHost() {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        try {
            tlsToolkitStandaloneCommandLine.parse("-K", testPassword1, "-K", testPassword2);
            fail("Expected error with mismatch keystore password number");
        } catch (CommandLineParseException e) {
            assertEquals(ExitCode.ERROR_INCORRECT_NUMBER_OF_PASSWORDS.ordinal(), e.getExitCode());
        }
    }

    @Test
    public void testTruststorePasswordArg() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitStandaloneCommandLine.parse("-P", testPassword);
        List<String> trustStorePasswords = tlsToolkitStandaloneCommandLine.getTrustStorePasswords();
        assertEquals(1, trustStorePasswords.size());
        assertEquals(testPassword, trustStorePasswords.get(0));
    }

    @Test
    public void testMultipleTruststorePasswordArgs() throws CommandLineParseException {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        tlsToolkitStandaloneCommandLine.parse("-n", "nifi1,nifi2", "-P", testPassword1, "-P", testPassword2);
        List<String> trustStorePasswords = tlsToolkitStandaloneCommandLine.getTrustStorePasswords();
        assertEquals(2, trustStorePasswords.size());
        assertEquals(testPassword1, trustStorePasswords.get(0));
        assertEquals(testPassword2, trustStorePasswords.get(1));
    }

    @Test
    public void testMultipleTruststorePasswordArgSingleHost() {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        try {
            tlsToolkitStandaloneCommandLine.parse("-P", testPassword1, "-P", testPassword2);
            fail("Expected error with mismatch keystore password number");
        } catch (CommandLineParseException e) {
            assertEquals(ExitCode.ERROR_INCORRECT_NUMBER_OF_PASSWORDS.ordinal(), e.getExitCode());
        }
    }

    private Properties getProperties() throws IOException {
        NiFiPropertiesWriter niFiPropertiesWriter = tlsToolkitStandaloneCommandLine.getNiFiPropertiesWriterFactory().create();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        niFiPropertiesWriter.writeNiFiProperties(byteArrayOutputStream);
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        return properties;
    }
}
