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

package org.apache.nifi.toolkit.tls.commandLine;

import org.apache.nifi.toolkit.tls.TlsToolkitMainTest;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.stubbing.defaultanswers.ForwardsInvocations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

public class TlsToolkitCommandLineTest {
    private SecureRandom secureRandom;
    private TlsToolkitCommandLine tlsToolkitCommandLine;

    @Before
    public void setup() {
        secureRandom = mock(SecureRandom.class);
        doAnswer(new ForwardsInvocations(new Random())).when(secureRandom).nextBytes(any(byte[].class));
        tlsToolkitCommandLine = new TlsToolkitCommandLine(secureRandom);
    }

    @Test
    public void testHelp() {
        try {
            tlsToolkitCommandLine.parse("-h");
            fail("Expected usage and help exit");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.HELP_EXIT_CODE, e.getExitCode());
        }
    }

    @Test
    public void testUnknownArg() {
        try {
            tlsToolkitCommandLine.parse("--unknownArg");
            fail("Expected error parsing command line");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_PARSING_COMMAND_LINE, e.getExitCode());
        }
    }

    @Test
    public void testKeyAlgorithm() throws CommandLineParseException {
        String testKeyAlgorithm = "testKeyAlgorithm";
        tlsToolkitCommandLine.parse("-a", testKeyAlgorithm);
        assertEquals(testKeyAlgorithm, tlsToolkitCommandLine.getTlsHelperConfig().getKeyPairAlgorithm());
    }

    @Test
    public void testKeySizeArgNotInteger() {
        try {
            tlsToolkitCommandLine.parse("-k", "badVal");
            fail("Expected bad keysize exit code");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_PARSING_INT_KEYSIZE, e.getExitCode());
        }
    }

    @Test
    public void testKeySize() throws CommandLineParseException {
        int testKeySize = 4096;
        tlsToolkitCommandLine.parse("-k", Integer.toString(testKeySize));
        assertEquals(testKeySize, tlsToolkitCommandLine.getTlsHelperConfig().getKeySize());
    }

    @Test
    public void testSigningAlgorithm() throws CommandLineParseException {
        String testSigningAlgorithm = "testSigningAlgorithm";
        tlsToolkitCommandLine.parse("-s", testSigningAlgorithm);
        assertEquals(testSigningAlgorithm, tlsToolkitCommandLine.getTlsHelperConfig().getSigningAlgorithm());
    }

    @Test
    public void testDaysNotInteger() {
        try {
            tlsToolkitCommandLine.parse("-d", "badVal");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_PARSING_INT_DAYS, e.getExitCode());
        }
    }

    @Test
    public void testDays() throws CommandLineParseException {
        int testDays = 29;
        tlsToolkitCommandLine.parse("-d", Integer.toString(testDays));
        assertEquals(testDays, tlsToolkitCommandLine.getTlsHelperConfig().getDays());
    }

    @Test
    public void testKeyStoreType() throws CommandLineParseException {
        String testKeyStoreType = "testKeyStoreType";
        tlsToolkitCommandLine.parse("-t", testKeyStoreType);
        assertEquals(testKeyStoreType, tlsToolkitCommandLine.getTlsHelperConfig().getKeyStoreType());
    }

    @Test
    public void testOutputDirectory() throws CommandLineParseException {
        String testPath = "/fake/path/doesnt/exist";
        tlsToolkitCommandLine.parse("-o", testPath);
        assertEquals(testPath, tlsToolkitCommandLine.getBaseDir().getAbsolutePath());
    }

    @Test
    public void testHostnames() throws CommandLineParseException {
        String nifi1 = "nifi1";
        String nifi2 = "nifi2";

        tlsToolkitCommandLine.parse("-n", nifi1 + " , " + nifi2);

        List<String> hostnames = tlsToolkitCommandLine.getHostnames();
        assertEquals(2, hostnames.size());
        assertEquals(nifi1, hostnames.get(0));
        assertEquals(nifi2, hostnames.get(1));
    }

    @Test
    public void testHttpsPort() throws CommandLineParseException {
        String testPort = "8998";
        tlsToolkitCommandLine.parse("-p", testPort);
        assertEquals(testPort, tlsToolkitCommandLine.getHttpsPort());
    }

    @Test
    public void testNifiPropertiesFile() throws CommandLineParseException, IOException {
        tlsToolkitCommandLine.parse("-f", TlsToolkitMainTest.TEST_NIFI_PROPERTIES);
        assertEquals(TlsToolkitMainTest.FAKE_VALUE, getProperties().get(TlsToolkitMainTest.NIFI_FAKE_PROPERTY));
    }

    @Test
    public void testNifiPropertiesFileDefault() throws CommandLineParseException, IOException {
        tlsToolkitCommandLine.parse();
        assertNull(getProperties().get(TlsToolkitMainTest.NIFI_FAKE_PROPERTY));
    }

    @Test
    public void testBadNifiPropertiesFile() {
        try {
            tlsToolkitCommandLine.parse("-f", "/this/file/should/not/exist.txt");
            fail("Expected error when unable to read file");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_READING_NIFI_PROPERTIES, e.getExitCode());
        }
    }

    @Test
    public void testNotSameKeyAndKeystorePassword() throws CommandLineParseException {
        tlsToolkitCommandLine.parse();
        List<String> keyStorePasswords = tlsToolkitCommandLine.getKeyStorePasswords();
        List<String> keyPasswords = tlsToolkitCommandLine.getKeyPasswords();
        assertEquals(1, tlsToolkitCommandLine.getHostnames().size());
        assertEquals(1, keyStorePasswords.size());
        assertEquals(1, keyPasswords.size());
        assertNotEquals(keyStorePasswords.get(0), keyPasswords.get(0));
    }

    @Test
    public void testSameKeyAndKeystorePassword() throws CommandLineParseException {
        tlsToolkitCommandLine.parse("-R");
        List<String> keyStorePasswords = tlsToolkitCommandLine.getKeyStorePasswords();
        List<String> keyPasswords = tlsToolkitCommandLine.getKeyPasswords();
        assertEquals(1, tlsToolkitCommandLine.getHostnames().size());
        assertEquals(1, keyStorePasswords.size());
        assertEquals(1, keyPasswords.size());
        assertEquals(keyStorePasswords.get(0), keyPasswords.get(0));
    }

    @Test
    public void testSameKeyAndKeystorePasswordWithKeystorePasswordSpecified() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitCommandLine.parse("-R", "-S", testPassword);
        List<String> keyStorePasswords = tlsToolkitCommandLine.getKeyStorePasswords();
        assertEquals(1, keyStorePasswords.size());
        assertEquals(testPassword, keyStorePasswords.get(0));
        assertEquals(keyStorePasswords, tlsToolkitCommandLine.getKeyPasswords());
    }

    @Test
    public void testSameKeyAndKeystorePasswordWithKeyPasswordSpecified() {
        try {
            tlsToolkitCommandLine.parse("-R", "-K", "testPassword");
            fail("Expected error when specifying same key and password with a key password");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_SAME_KEY_AND_KEY_PASSWORD, e.getExitCode());
        }
    }

    @Test
    public void testKeyStorePasswordArg() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitCommandLine.parse("-S", testPassword);
        List<String> keyStorePasswords = tlsToolkitCommandLine.getKeyStorePasswords();
        assertEquals(1, keyStorePasswords.size());
        assertEquals(testPassword, keyStorePasswords.get(0));
    }

    @Test
    public void testMultipleKeystorePasswordArgs() throws CommandLineParseException {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        tlsToolkitCommandLine.parse("-n", "nifi1,nifi2", "-S", testPassword1, "-S", testPassword2);
        List<String> keyStorePasswords = tlsToolkitCommandLine.getKeyStorePasswords();
        assertEquals(2, keyStorePasswords.size());
        assertEquals(testPassword1, keyStorePasswords.get(0));
        assertEquals(testPassword2, keyStorePasswords.get(1));
    }

    @Test
    public void testMultipleKeystorePasswordArgSingleHost() {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        try {
            tlsToolkitCommandLine.parse("-S", testPassword1, "-S", testPassword2);
            fail("Expected error with mismatch keystore password number");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_INCORRECT_NUMBER_OF_PASSWORDS, e.getExitCode());
        }
    }

    @Test
    public void testKeyPasswordArg() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitCommandLine.parse("-K", testPassword);
        List<String> keyPasswords = tlsToolkitCommandLine.getKeyPasswords();
        assertEquals(1, keyPasswords.size());
        assertEquals(testPassword, keyPasswords.get(0));
    }

    @Test
    public void testMultipleKeyPasswordArgs() throws CommandLineParseException {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        tlsToolkitCommandLine.parse("-n", "nifi1,nifi2", "-K", testPassword1, "-K", testPassword2);
        List<String> keyPasswords = tlsToolkitCommandLine.getKeyPasswords();
        assertEquals(2, keyPasswords.size());
        assertEquals(testPassword1, keyPasswords.get(0));
        assertEquals(testPassword2, keyPasswords.get(1));
    }

    @Test
    public void testMultipleKeyPasswordArgSingleHost() {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        try {
            tlsToolkitCommandLine.parse("-K", testPassword1, "-K", testPassword2);
            fail("Expected error with mismatch keystore password number");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_INCORRECT_NUMBER_OF_PASSWORDS, e.getExitCode());
        }
    }

    @Test
    public void testTruststorePasswordArg() throws CommandLineParseException {
        String testPassword = "testPassword";
        tlsToolkitCommandLine.parse("-T", testPassword);
        List<String> trustStorePasswords = tlsToolkitCommandLine.getTrustStorePasswords();
        assertEquals(1, trustStorePasswords.size());
        assertEquals(testPassword, trustStorePasswords.get(0));
    }

    @Test
    public void testMultipleTruststorePasswordArgs() throws CommandLineParseException {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        tlsToolkitCommandLine.parse("-n", "nifi1,nifi2", "-T", testPassword1, "-T", testPassword2);
        List<String> trustStorePasswords = tlsToolkitCommandLine.getTrustStorePasswords();
        assertEquals(2, trustStorePasswords.size());
        assertEquals(testPassword1, trustStorePasswords.get(0));
        assertEquals(testPassword2, trustStorePasswords.get(1));
    }

    @Test
    public void testMultipleTruststorePasswordArgSingleHost() {
        String testPassword1 = "testPassword1";
        String testPassword2 = "testPassword2";
        try {
            tlsToolkitCommandLine.parse("-T", testPassword1, "-T", testPassword2);
            fail("Expected error with mismatch keystore password number");
        } catch (CommandLineParseException e) {
            assertEquals(TlsToolkitCommandLine.ERROR_INCORRECT_NUMBER_OF_PASSWORDS, e.getExitCode());
        }
    }

    private Properties getProperties() throws IOException {
        NiFiPropertiesWriter niFiPropertiesWriter = tlsToolkitCommandLine.getNiFiPropertiesWriterFactory().create();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        niFiPropertiesWriter.writeNiFiProperties(byteArrayOutputStream);
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        return properties;
    }
}
