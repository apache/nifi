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
package org.apache.nifi.tests.system;

import org.apache.nifi.bootstrap.RunNiFi;
import org.apache.nifi.registry.security.util.KeystoreType;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientConfig;
import org.apache.nifi.toolkit.cli.impl.client.nifi.impl.JerseyNiFiClient;
import org.apache.nifi.util.file.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class SpawnedStandaloneNiFiInstanceFactory implements NiFiInstanceFactory {
    private final InstanceConfiguration instanceConfig;

    public SpawnedStandaloneNiFiInstanceFactory(final InstanceConfiguration instanceConfig) {
        this.instanceConfig = instanceConfig;
    }

    @Override
    public NiFiInstance createInstance() {
        return new RunNiFiInstance(instanceConfig);
    }


    private static class RunNiFiInstance implements NiFiInstance {
        private final File instanceDirectory;
        private final File configDir;
        private final InstanceConfiguration instanceConfiguration;
        private File bootstrapConfigFile;
        private RunNiFi runNiFi;

        public RunNiFiInstance(final InstanceConfiguration instanceConfiguration) {
            this.instanceDirectory = instanceConfiguration.getInstanceDirectory();
            this.bootstrapConfigFile = instanceConfiguration.getBootstrapConfigFile();
            this.instanceConfiguration = instanceConfiguration;

            final Properties bootstrapProperties = new Properties();
            try (final InputStream fis = new FileInputStream(bootstrapConfigFile)) {
                bootstrapProperties.load(fis);
            } catch (final IOException e) {
                throw new RuntimeException("Could not load boostrap config file " + bootstrapConfigFile, e);
            }

            final String confDirName = bootstrapProperties.getProperty("conf.dir");
            final File tempConfDir = new File(confDirName);
            if (tempConfDir.isAbsolute()) {
                configDir = tempConfDir;
            } else {
                configDir = new File(instanceDirectory, confDirName);
            }
        }

        public String toString() {
            return "RunNiFiInstance[dir=" + instanceDirectory + "]";
        }

        @Override
        public void start(final boolean waitForCompletion) {
            if (runNiFi != null) {
                throw new IllegalStateException("NiFi has already been started");
            }

            System.out.println("Starting instance " + instanceDirectory.getName());

            try {
                this.runNiFi = new RunNiFi(bootstrapConfigFile);
            } catch (IOException e) {
                throw new RuntimeException("Failed to start NiFi", e);
            }

            try {
                runNiFi.start(false);

                if (waitForCompletion) {
                    waitForStartup();
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to start NiFi", e);
            }
        }

        public void createEnvironment() throws IOException {
            System.out.println("Creating environment for instance " + instanceDirectory.getName());

            cleanup();

            final File destinationConf = new File(instanceDirectory, "conf");
            copyContents(bootstrapConfigFile.getParentFile(), destinationConf);
            bootstrapConfigFile = new File(destinationConf, bootstrapConfigFile.getName());

            final File destinationLib = new File(instanceDirectory, "lib");
            copyContents(new File("target/nifi-lib-assembly/lib"), destinationLib);

            final File destinationCertsDir = new File(instanceDirectory, "certs");
            if (!destinationCertsDir.exists()) {
                assertTrue(destinationCertsDir.mkdirs());
            }

            // Copy keystore
            final File destinationKeystore = new File(destinationCertsDir, "keystore.jks");
            Files.copy(Paths.get("src/test/resources/keystore.jks"), destinationKeystore.toPath());

            // Copy truststore
            final File destinationTruststore = new File(destinationCertsDir, "truststore.jks");
            Files.copy(Paths.get("src/test/resources/truststore.jks"), destinationTruststore.toPath());

            final File flowXmlGz = instanceConfiguration.getFlowXmlGz();
            if (flowXmlGz != null) {
                final File destinationFlowXmlGz = new File(destinationConf, "flow.xml.gz");
                Files.copy(flowXmlGz.toPath(), destinationFlowXmlGz.toPath());
            }

            // Write out any Property overrides
            final Map<String, String> nifiPropertiesOverrides = instanceConfiguration.getNifiPropertiesOverrides();
            if (nifiPropertiesOverrides != null && !nifiPropertiesOverrides.isEmpty()) {
                final File destinationNifiProperties = new File(destinationConf, "nifi.properties");
                final File sourceNifiProperties = new File(bootstrapConfigFile.getParentFile(), "nifi.properties");

                final Properties nifiProperties = new Properties();
                try (final InputStream fis = new FileInputStream(sourceNifiProperties)) {
                    nifiProperties.load(fis);
                }

                nifiPropertiesOverrides.forEach(nifiProperties::setProperty);

                try (final OutputStream fos = new FileOutputStream(destinationNifiProperties)) {
                    nifiProperties.store(fos, null);
                }
            }
        }

        private void copyContents(final File dir, final File destinationDir) throws IOException {
            if (!destinationDir.exists()) {
                assertTrue(destinationDir.mkdirs());
            }

            final File[] sourceFiles = dir.listFiles();
            for (final File sourceFile : sourceFiles) {
                if (sourceFile.isDirectory()) {
                    final File destinationFile = new File(destinationDir, sourceFile.getName());
                    copyContents(sourceFile, destinationFile);
                    continue;
                }

                final File destinationFile = new File(destinationDir, sourceFile.getName());
                if (destinationFile.exists()) {
                    assertTrue(destinationFile.delete());
                }

                Files.copy(sourceFile.toPath(), destinationFile.toPath());
            }
        }

        @Override
        public void setFlowXmlGz(final File flowXmlGz) throws IOException {
            final File destinationConf = new File(instanceDirectory, "conf");
            final File destinationFlowXmlGz = new File(destinationConf, "flow.xml.gz");
            destinationFlowXmlGz.delete();
            Files.copy(flowXmlGz.toPath(), destinationFlowXmlGz.toPath());
        }

        private void waitForStartup() throws IOException {
            final NiFiClient client = createClient();

            while (true) {
                try {
                    client.getFlowClient().getRootGroupId();
                    System.out.println("Completed startup of instance " + instanceDirectory.getName());
                    return;
                } catch (final Exception e) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException ex) {
                    }

                    continue;
                }
            }
        }

        @Override
        public void stop() {
            if (runNiFi == null) {
                return;
            }

            System.out.println("Stopping instance " + instanceDirectory.getName());

            try {
                runNiFi.stop();
                System.out.println("Completed shutdown of instance " + instanceDirectory.getName());
            } catch (IOException e) {
                throw new RuntimeException("Failed to stop NiFi", e);
            } finally {
                runNiFi = null;
            }
        }


        private void cleanup() throws IOException {
            if (instanceDirectory.exists()) {
                FileUtils.deleteFile(instanceDirectory, true);
            }
        }

        @Override
        public boolean isClustered() {
            return false;
        }

        @Override
        public int getNumberOfNodes() {
            return 1;
        }

        @Override
        public int getNumberOfNodes(final boolean includeOnlyAutoStartInstances) {
            return isAutoStart() ? 1 : 0;
        }

        @Override
        public NiFiInstance getNodeInstance(final int nodeIndex) {
            return null;
        }

        @Override
        public Properties getProperties() throws IOException {
            final File nifiPropsFile = new File(configDir, "nifi.properties");
            final Properties nifiProps = new Properties();

            try (final InputStream fis = new FileInputStream(nifiPropsFile)) {
                nifiProps.load(fis);
            }

            return nifiProps;
        }

        @Override
        public File getInstanceDirectory() {
            return instanceDirectory;
        }

        @Override
        public boolean isAutoStart() {
            return instanceConfiguration.isAutoStart();
        }

        @Override
        public void setProperty(final String propertyName, final String propertyValue) throws IOException {
            setProperties(Collections.singletonMap(propertyName, propertyValue));
        }

        @Override
        public void setProperties(final Map<String, String> properties) throws IOException {
            final Properties currentProperties = getProperties();
            currentProperties.putAll(properties);

            final File propertiesFile = new File(configDir, "nifi.properties");
            try (final OutputStream fos = new FileOutputStream(propertiesFile)) {
                currentProperties.store(fos, "");
            }
        }

        public NiFiClient createClient() throws IOException {
            final Properties nifiProperties = getProperties();
            final String httpPort = nifiProperties.getProperty("nifi.web.http.port");
            final String httpsPort = nifiProperties.getProperty("nifi.web.https.port");
            final String webPort = (httpsPort == null || httpsPort.trim().isEmpty()) ? httpPort : httpsPort;

            final String keystoreType = nifiProperties.getProperty("nifi.security.keystoreType");
            final String truststoreType = nifiProperties.getProperty("nifi.security.truststoreType");

            final NiFiClientConfig clientConfig = new NiFiClientConfig.Builder()
                .baseUrl("http://localhost:" + webPort)
                .connectTimeout(30000)
                .readTimeout(30000)
                .keystoreFilename(getAbsolutePath(nifiProperties.getProperty("nifi.security.keystore")))
                .keystorePassword(nifiProperties.getProperty("nifi.security.keystorePasswd"))
                .keystoreType(keystoreType == null ? null : KeystoreType.valueOf(keystoreType))
                .truststoreFilename(getAbsolutePath(nifiProperties.getProperty("nifi.security.truststore")))
                .truststorePassword(nifiProperties.getProperty("nifi.security.truststorePasswd"))
                .truststoreType(truststoreType == null ? null : KeystoreType.valueOf(truststoreType))
                .build();

            return new JerseyNiFiClient.Builder()
                .config(clientConfig)
                .build();
        }

        private String getAbsolutePath(final String filename) {
            if (filename == null) {
                return null;
            }

            final File file = new File(filename);
            if (file.isAbsolute()) {
                return file.getAbsolutePath();
            }

            return new File(instanceDirectory, file.getPath()).getAbsolutePath();
        }
    }
}
