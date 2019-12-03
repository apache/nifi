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
package org.apache.nifi.properties.sensitive.hadoop;


import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.AbstractSensitivePropertyProviderTest;
import org.apache.nifi.properties.sensitive.CipherUtils;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.StandardSensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.keystore.KeyStoreWrappedSensitivePropertyProvider;
import org.apache.nifi.security.util.CertificateUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;


public class HadoopCredentialsSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static SecureRandom random = new SecureRandom();
    private String keyAlias;
    private SecretKeySpec keySpec;
    private KeyStore keyStore;
    private byte[] keyMaterial = new byte[27];
    private String validKeyStorePath;

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();

    // NB:  we have to perform this setup once per-test, not once per-class (via @BeforeClass) because one of the tests
    // removes the file explicitly, since we can't rely on test order we have to ensure the file is replaced for each test.
    @Before
    public void setUpValidKeyStore() throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        random.nextBytes(keyMaterial);
        keyStore = KeyStore.getInstance(KeyStoreWrappedSensitivePropertyProvider.KEYSTORE_TYPE_JCECKS);
        keyStore.load(null, null);

        keyAlias = CipherUtils.getRandomHex(8);
        keySpec = new SecretKeySpec(keyMaterial, "AES");
        keyStore.setKeyEntry(keyAlias, keySpec, HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PASSWORD_DEFAULT.toCharArray(), null);

        File tmp = tmpDir.newFile();
        tmp.deleteOnExit();
        validKeyStorePath = tmp.getAbsolutePath();
        saveKeyStore();
    }

    @Before
    public void setUpSystemProperties() {
        System.setProperty(HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PATHS_SYS_PROP, validKeyStorePath);
    }

    // This just shows our class setup worked as expected + all of our prerequisites are met.
    @Test
    public void testPrerequisites() {
        Assert.assertNotNull(keyStore);
        Assert.assertNotNull(keySpec);
        Assert.assertNotNull(keyAlias);
    }

    // This test shows that the provider throws an exception when called to protect a value.
    @Test
    public void testBasicProtectValue() throws Exception {
        HadoopCredentialsSensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();
        String value = CipherUtils.getRandomHex(8);
        try {
            Assert.assertEquals(value, spp.protect(value));
            throw new Exception("SensitivePropertyProtectionException expected");
        } catch (final SensitivePropertyProtectionException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "Provider cannot protect values"));
        }
    }

    // This test shows that the provider returns the value from the key store as expected.
    @Test
    public void testBasicUnprotectValue() {
        SensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();
        String keyEncoded = StringUtils.toEncodedString(keyMaterial, Charset.defaultCharset());

        // Let's be sure we've not mixed up our values:
        Assert.assertNotEquals(keyEncoded, keyAlias);

        // This shows the value we put in originally is now retrievable via the alias:
        Assert.assertEquals(keyEncoded, spp.unprotect(keyAlias));
    }


    // This test shows that blank value handling is as expected by using the base class helper method.
    @Test
    public void testBlankValueBehavior() throws Exception {
        checkProviderProtectDoesNotAllowBlankValues(new HadoopCredentialsSensitivePropertyProvider());
    }


    // This test shows that a good key store filename is handled correctly even when missing or invalid paths are included.
    @Test
    public void testPathHandling() {
        String invalidKeyStorePath = "-+ +=!";
        String missingKeyStorePath = "/tmp/" + CipherUtils.getRandomHex(8);
        String[][] pathGroups = new String[][]{
                new String[]{validKeyStorePath, invalidKeyStorePath, missingKeyStorePath},
                new String[]{invalidKeyStorePath, missingKeyStorePath, validKeyStorePath},
                new String[]{missingKeyStorePath, validKeyStorePath, invalidKeyStorePath},
                new String[]{null, validKeyStorePath},
                new String[]{validKeyStorePath}
        };

        String value = StringUtils.toEncodedString(keyMaterial, Charset.defaultCharset());
        for (String[] group : pathGroups) {
            SensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider(StringUtils.join(group, HadoopCredentialsSensitivePropertyProvider.PATH_SEPARATOR));
            Assert.assertEquals(value, spp.unprotect(keyAlias));
        }
    }


    // This test shows that we can ask the provider class if it supports various spec strings and it responds as expected.
    @Test
    public void testProviderFor() {
        Assert.assertTrue(HadoopCredentialsSensitivePropertyProvider.isProviderFor("hadoop/file"));
        Assert.assertFalse(HadoopCredentialsSensitivePropertyProvider.isProviderFor("hadoop/"));
        Assert.assertFalse(HadoopCredentialsSensitivePropertyProvider.isProviderFor(""));
        Assert.assertFalse(HadoopCredentialsSensitivePropertyProvider.isProviderFor("other/hadoop/value"));
    }


    // This test shows how keys are printed by this provider.
    @Test
    public void testPrintableString() {
        String value = CipherUtils.getRandomHex(32);
        Assert.assertEquals(value, HadoopCredentialsSensitivePropertyProvider.toPrintableString(value));
    }


    // This test shows how the provider behaves when the key store is deleted during its lifecycle.
    @Test
    public void testDeletedKeyStoreFile() {
        String value = StringUtils.toEncodedString(keyMaterial, Charset.defaultCharset());
        SensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();

        Assert.assertEquals(value, spp.unprotect(keyAlias));
        new File(validKeyStorePath).delete();

        try {
            Assert.assertEquals(value, spp.unprotect(keyAlias));
        } catch (final SensitivePropertyProtectionException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "No key store found"));
        }
    }


    // This test shows that we get an HadoopCredentialsSensitivePropertyProvider when we ask the standard sensitive property provider for one.
    @Test
    public void testStandardSensitivePropertyProviderReturnsHadoopProvider() {
        SensitivePropertyProvider spp = StandardSensitivePropertyProvider.fromKey("hadoop/" + validKeyStorePath);
        Assert.assertNotNull(spp);
        Assert.assertTrue(spp instanceof HadoopCredentialsSensitivePropertyProvider);
    }


    // This test shows that we can format a value as expected.
    @Test
    public void testFormatForType() {
        String paths = "a,b,c";
        String formatted = HadoopCredentialsSensitivePropertyProvider.formatForType(paths);
        Assert.assertTrue(HadoopCredentialsSensitivePropertyProvider.isProviderFor(formatted));

        paths = "";
        formatted = HadoopCredentialsSensitivePropertyProvider.formatForType(paths);
        Assert.assertFalse(HadoopCredentialsSensitivePropertyProvider.isProviderFor(formatted));
    }


    // This test shows how the provider behaves when the store is valid but the key is gone.
    @Test
    public void testValidStoreMissingKey() throws Exception {
        HadoopCredentialsSensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();

        // Delete the key by removing it and re-saving the store.
        keyStore.deleteEntry(keyAlias);
        saveKeyStore();

        try {
            spp.unprotect(keyAlias);
            throw new Exception("SensitivePropertyProtectionException should have been thrown but was not.");
        } catch (final SensitivePropertyProtectionException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "Value not found at key."));
        }
    }


    // This test shows how the provider behaves when the store is valid but the value at the alias isn't a key.
    @Test
    public void testValidStorePasswordWrongEntryType() throws Exception {
        HadoopCredentialsSensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();

        // Remove the alias, re-create it as a certificate, and re-save.
        keyStore.deleteEntry(keyAlias);
        KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        Certificate cert = CertificateUtils.generateSelfSignedX509Certificate(keyPair, "CN=testca,O=Apache,OU=NiFi", "SHA256withRSA", 365);
        keyStore.setCertificateEntry(keyAlias, cert);
        saveKeyStore();

        try {
            spp.unprotect(keyAlias);
            throw new Exception("SensitivePropertyProtectionException should have been thrown but was not.");
        } catch (final SensitivePropertyProtectionException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "Value not found at key."));
        }
    }


    // This test shows how the provider behaves when the store is valid but the key is password protected.
    @Test
    public void testValidStorePasswordProtectedKey() throws Exception {
        HadoopCredentialsSensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();

        // Remove the alias and re-create it from the same key material but with a random password.
        keyStore.deleteEntry(keyAlias);
        keyStore.setKeyEntry(keyAlias, keySpec, CipherUtils.getRandomHex(16).toCharArray(), null);
        saveKeyStore();

        try {
            spp.unprotect(keyAlias);
            throw new Exception("SensitivePropertyProtectionException should have been thrown but was not.");
        } catch (final SensitivePropertyProtectionException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "Given final block not properly padded."));
        }

    }


    // This helper method overwrites the existing key store file with the current contents.
    private void saveKeyStore() throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        ByteArrayOutputStream storeOutput = new ByteArrayOutputStream();
        keyStore.store(storeOutput, HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PASSWORD_DEFAULT.toCharArray());
        OutputStream fos = new FileOutputStream(validKeyStorePath);
        fos.write(storeOutput.toByteArray());
        fos.close();
    }


    /**
     * This test shows we can load a value from a known-good keystore as created with the hadoop command.
     *
     * To create this file, run this command from the `nifi/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-properties-loader` directory:
     *
     * $ hadoop credential create test-key  -value test-value -provider jceks://file/`pwd`/src/test/resources/hadoop_keystores/example.jceks
     */
    @Test
    public void testKnownGoodExternalKeyStore() {
        File keyStoreFile = new File(getClass().getClassLoader().getResource("hadoop_keystores/example.jceks").getFile());
        System.setProperty(HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PATHS_SYS_PROP, keyStoreFile.getAbsolutePath());

        SensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();
        Assert.assertEquals(spp.unprotect("test-key"), "test-value");
    }


    /**
     * This test shows we can load a value from a known-good keystore that has an associated password file.
     *
     * To create the files for this test, run these commands (same directory as above):
     *
     * $ echo -n "swordfish" > src/test/resources/hadoop_keystores/password.sidefile
     * $ HADOOP_CREDSTORE_PASSWORD=swordfish hadoop credential create test-key -value test-value -provider jceks://file/`pwd`/src/test/resources/hadoop_keystores/with-sidefile.jceks
     */
    @Test
    public void testKnownGoodExternalKeyStoreWithPasswordFile() {
        File keyStoreFile = new File(getClass().getClassLoader().getResource("hadoop_keystores/with-sidefile.jceks").getFile());
        System.setProperty(HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PATHS_SYS_PROP, keyStoreFile.getAbsolutePath());

        File passwordFile = new File(getClass().getClassLoader().getResource("hadoop_keystores/password.sidefile").getFile());
        System.setProperty(HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PASSWORD_FILE_SYS_PROP, passwordFile.getAbsolutePath());

        SensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();
        Assert.assertEquals(spp.unprotect("test-key"), "test-value");
    }

    /**
     * This test shows we cannot load a value from a known-good keystore that has an associated password file, where the password file contains the wrong password.
     *
     * To create the files for this test, run these commands (same directory as above):
     *
     * $ echo -n "invalid" > src/test/resources/hadoop_keystores/bad-password.sidefile
     * $ HADOOP_CREDSTORE_PASSWORD=swordfish hadoop credential create test-key -value test-value -provider jceks://file/`pwd`/src/test/resources/hadoop_keystores/with-sidefile.jceks
     *
     * The second command may be skipped if the file was created previously (as per the comments in the test directly above this one).  If the file exists
     * and the command is re-run, it will error out harmlessly.
     *
     */
    @Test
    public void testKnownGoodExternalKeyStoreWithBadPasswordFile() throws Exception {
        File keyStoreFile = new File(getClass().getClassLoader().getResource("hadoop_keystores/with-sidefile.jceks").getFile());
        System.setProperty(HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PATHS_SYS_PROP, keyStoreFile.getAbsolutePath());

        File passwordFile = new File(getClass().getClassLoader().getResource("hadoop_keystores/bad-password.sidefile").getFile());
        System.setProperty(HadoopCredentialsSensitivePropertyProvider.KEYSTORE_PASSWORD_FILE_SYS_PROP, passwordFile.getAbsolutePath());

        SensitivePropertyProvider spp = new HadoopCredentialsSensitivePropertyProvider();
        try {
            spp.unprotect("test-key");
            throw new Exception("SensitivePropertyProtectionException should have been thrown but was not.");
        } catch (final SensitivePropertyProtectionException e) {
            Assert.assertTrue(StringUtils.contains(e.getMessage(), "password incorrect"));
        }
    }
}
