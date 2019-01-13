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



package org.apache.nifi.testharness;

import org.apache.nifi.EmbeddedNiFi;
import org.apache.nifi.testharness.api.FlowFileEditorCallback;
import org.apache.nifi.testharness.util.FileUtils;
import org.apache.nifi.testharness.util.NiFiCoreLibClassLoader;
import org.apache.nifi.testharness.util.XmlUtils;
import org.apache.nifi.testharness.util.Zip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipEntry;

/**
 * <p>
 * An API wrapper of a "test" NiFi instance to which a flow definition is installed for testing.</p>
 *
 * <p>
 * Due to NiFi design restrictions, {@code TestNiFiInstance} has to take <i>full command</i>
 * of the current working directory: it installs a full NiFi installation to there. To ensure
 * this is desired, <strong>it will only run if the current directory is called
 * "nifi_testharness_nifi_home"</strong>. As such the JVM process has to be started inside a directory
 * called "nifi_testharness_nifi_home" so that the following is true:
 *
 * <pre><tt>
 *     new File(System.getProperty("user.dir")).getName().equals("nifi_testharness_nifi_home")
 * </tt></pre>
 * </p>
 *
 * <p>
 * Before {@code TestNiFiInstance} can be used, it has to be configured via its builder
 * interface:
 * <ul>
 *     <li>
 *      {@link Builder#setNiFiBinaryDistributionZip(File)} specifies the location of the NiFi binary
 *      distribution ZIP file to be used.
 *     </li>
 *      <li>
 *      {@link Builder#setFlowXmlToInstallForTesting(File)} specifies the location of the NiFi flow
 *      to install.
 *     </li>
 *     <li>
 *      {@link Builder#modifyFlowXmlBeforeInstalling(FlowFileEditorCallback)} allows on-the-fly
 *      changes to be performed to the Flow file before it is actually installed.
 *     </li>
 * </ul>
 *
 * <h5>Sample</h5>
 * <pre><tt>
 * TestNiFiInstance testNiFiInstance = TestNiFiInstance.builder()
 *      .setNiFiBinaryDistributionZip(YourConstants.NIFI_ZIP_FILE)
 *      .setFlowXmlToInstallForTesting(YourConstants.FLOW_XML_FILE)
 *      .modifyFlowXmlBeforeInstalling(YourConstants.FLOW_FILE_CHANGES_FOR_TESTS)
 *      .build();
 * </tt></pre>
 *
 * </p>
 *
 * <p>
 * If the current working directory is called "nifi_testharness_nifi_home", the caller can
 * {@link #install()} this {@code TestNiFiInstance}, which will
 * <ol>
 *  <li>
 *      (as a first cleanup step) erase all content of the current working directory.
 *      (NOTE: this potentially destructive operation is the reason why we have the
 *      "nifi_testharness_nifi_home" directory name guard in place!)
 *  </li>
 *  <li>
 *      Extracts the contents of the NiFi binary distribution ZIP file specified in
 *      the configuration to a to a temporary directory.
 *  <li>
 *      Symlinks all files from the temporary directory to the current working
 *      directory, causing the directory to hold a fully functional
 *      NiFi installation.
 *  </li>
 *  <li>
 *      Installs the flow definition files(s) to the NiFi instance specified in
 *      the configuration.
 *  </li>
 * </ol>
 * </p>
 *
 * <p>
 *
 * The caller then can proceed to {@link #start()} this {@code TestNiFiInstance},
 * which will bootstrap the NiFi engine, which in turn will pick up and start processing
 * the flow definition supplied by the caller in the configuration.
 * </p>
 *
 * <p>
 * Once the previous step is done, the caller can perform asserts regarding the observed behaviour
 * of the NiFi flow, just like one would do it with standard Java test cases.
 * </p>
 *
 * <p>
 * To perform a clean shutdown of the hosted NiFi instance, the caller is required to call
 * {@link #stopAndCleanup()}, which will shut down NiFi and remove all temporary files, including
 * the symlinks created in the current working directory.
 * </p>
 *
 *
 * <h4>NOTES</h4>
 * <ul>
 *  <li>
 *      {@code TestNiFiInstance} is NOT thread safe: if more than one thread uses it,
 *      external synchronisation is required.
 *  </li>
 *   <li>
 *      Only one {@code TestNiFiInstance} can be started in the same "nifi_testharness_nifi_home"
 *      directory at the same time.
 *  </li>
 *  <li>
 *      Currently, due to NiFi limitations, one {@code TestNiFiInstance} can be started per JVM process.
 *      If multiple test cases are required, launch a new JVM process per test case
 *      (in sequence, see the point above): Maven/Surefire has built-in support for this.
 *  </li>
 * </ul>
 *
 * <p>
 * <strong>CAUTION: THIS IS AN EXPERIMENTAL API: EXPECT CHANGES!</strong>
 * Efforts will be made to retain backwards API compatibility, but
 * no guarantee is given.
 * </p>
 *
 *
 * @see TestNiFiInstance#builder()
 *
 *
 */
public class TestNiFiInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestNiFiInstance.class);


    private EmbeddedNiFi testNiFi;

    private final File nifiHomeDir;
    private final File bootstrapLibDir;

    private File nifiProperties;

    private final File flowXmlGz;

    private final File placeholderNiFiHomeDir;

    private String nifiVersion;


    private enum State {
        STOPPED,
        STOP_FAILED,
        START_FAILED(STOPPED),
        STARTED(STOPPED, STOP_FAILED),
        INSTALLATION_FAILED(),
        FLOW_INSTALLED(STARTED, START_FAILED),
        INSTALLED(FLOW_INSTALLED, INSTALLATION_FAILED),
        CREATED(INSTALLED, INSTALLATION_FAILED);


        private final Set<State> allowedTransitions;

        State(State... allowedTransitions) {
            this.allowedTransitions = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(allowedTransitions)));
        }

        private void checkCanTransition(State newState) {
            if (!this.allowedTransitions.contains(newState)) {
                throw new IllegalStateException("Cannot transition from " + this + " to " + newState);
            }
        }
    }

    private State currentState = State.CREATED;

    private final File nifiBinaryZip;
    private final File flowXml;
    private final FlowFileEditorCallback editCallback;

    private TestNiFiInstance(File nifiBinaryZip, File flowXml, FlowFileEditorCallback editCallback) {
        this.nifiBinaryZip = Objects.requireNonNull(nifiBinaryZip, "nifiBinaryZip");
        this.flowXml = Objects.requireNonNull(flowXml, "flowXml");
        this.editCallback = editCallback;

        nifiHomeDir = requireCurrentWorkingDirectoryIsCorrect();

        final File configDir = new File(nifiHomeDir, "conf");
        final File libDir = new File(nifiHomeDir, "lib");

        bootstrapLibDir = new File(libDir, "bootstrap");

        nifiProperties = new File(configDir, "nifi.properties");

        flowXmlGz = new File(configDir, "flow.xml.gz");

        placeholderNiFiHomeDir = requireCurrentWorkingDirectoryIsCorrect();
    }

    String getNifiVersion() {
        switch (currentState) {
            case INSTALLED:
            case FLOW_INSTALLED:
            case STARTED:
            case START_FAILED:
            case STOP_FAILED:
            case STOPPED:

                return Objects.requireNonNull(nifiVersion, "nifiVersion is null");

            default:
                throw new IllegalStateException(
                        "NiFi version can only be retrieved after a successful installation, not in: "
                                + currentState);
        }
    }

    public void install() throws IOException {

        currentState.checkCanTransition(State.INSTALLED);

        File[] staleInstallations = placeholderNiFiHomeDir.listFiles((dir, name) -> name.startsWith("nifi-"));
        if (staleInstallations != null) {
            Arrays.stream(staleInstallations).forEach(TestNiFiInstance::deleteFileOrDirectoryRecursively);
        }

        Path tempDirectory = null;
        try {
            tempDirectory = Files.createTempDirectory("installable-flow");



            LOGGER.info("Uncompressing NiFi archive {} to {} ...", nifiBinaryZip, placeholderNiFiHomeDir);

            Zip.unzipFile(nifiBinaryZip, placeholderNiFiHomeDir, new Zip.StatusListenerAdapter() {
                @Override
                public void onUncompressDone(ZipEntry ze) {
                    LOGGER.debug("Uncompressed {}", ze.getName());
                }
            });

            LOGGER.info("Uncompressing DONE");

            File actualNiFiHomeDir = getActualNiFiHomeDir(placeholderNiFiHomeDir);

            nifiVersion = getNiFiVersion(actualNiFiHomeDir);

            currentState = State.INSTALLED;

            File installableFlowFile = createInstallableFlowFile(tempDirectory);

            validateNiFiVersionAgainstFlowVersion(nifiVersion, installableFlowFile);

            FileUtils.createSymlinks(placeholderNiFiHomeDir, actualNiFiHomeDir);

            installFlowFile(installableFlowFile);
        } catch (Exception e) {

            currentState = State.INSTALLATION_FAILED;

            throw new RuntimeException("Installation failed: " + e.getMessage(), e);

        } finally {
            if (tempDirectory != null) {
                FileUtils.deleteDirectoryRecursive(tempDirectory);
            }
        }

        currentState = State.FLOW_INSTALLED;
    }

    private File createInstallableFlowFile(Path tempDirectory) throws IOException {

        File flowXmlFile = new File(tempDirectory.toFile(), "flow.xml");

        if (editCallback == null) {
            Files.copy(flowXml.toPath(), flowXmlFile.toPath());
        } else {
            if (editCallback instanceof TestNiFiInstanceAware) {
                ((TestNiFiInstanceAware)editCallback).setTestNiFiInstance(this);
            }

            XmlUtils.editXml(flowXml, flowXmlFile, editCallback);
        }

        return flowXmlFile;
    }

    private void installFlowFile(File fileToIncludeInGz) throws IOException {
        Zip.gzipFile(fileToIncludeInGz, flowXmlGz);
    }

    private static String getNiFiVersion(File nifiInstallDir) {

        File libDir = new File(nifiInstallDir, "lib");
        if (!libDir.exists()) {
            throw new IllegalStateException(
                    "No \"lib\" directory found in NiFi home directory: " + nifiInstallDir);
        }

        File[] nifiApiJarLookupResults =
                libDir.listFiles((dir, name) -> name.startsWith("nifi-api-") && name.endsWith(".jar"));

        if (nifiApiJarLookupResults == null) {
            // since we check the existence before, this can only be null in case of an I/O error
            throw new IllegalStateException(
                    "I/O error listing NiFi lib directory: " + libDir);
        }

        if (nifiApiJarLookupResults.length == 0) {
            throw new IllegalStateException(
                    "No \"\"nifi-api-*.jar\" file found in NiFi lib directory: " + libDir);
        }

        if (nifiApiJarLookupResults.length != 1) {
            throw new IllegalStateException(
                    "Multiple \"nifi-api-*.jar\" files found in NiFi lib directory: " + libDir);
        }

        File nifiApiJar = nifiApiJarLookupResults[0];


        return nifiApiJar.getName()
                .replace("nifi-api-", "")
                .replace(".jar", "");
    }

    private static void validateNiFiVersionAgainstFlowVersion(String nifiVersion, File flowFile) {

        String flowFileVersion = extractFlowFileVersion(flowFile);

        if (flowFileVersion != null
                && !flowFileVersion.equalsIgnoreCase(nifiVersion)) {

            // prevent user errors and fail fast in case we detect that the flow file
            // was created by a different version of NiFi. This can prevent a lot of confusion!

            throw new RuntimeException(String.format(
                    "The NiFi version referenced in the flow file ('%s') does not match the version of NiFi being used ('%s')",
                    flowFileVersion, nifiVersion));
        }
    }

    private static String extractFlowFileVersion(File flowFile) {

        Document flowDocument = XmlUtils.getFileAsDocument(flowFile);

        XPath xpath = XPathFactory.newInstance().newXPath();

        try {
            NodeList processorNodeVersion = (NodeList)
                    xpath.evaluate("//bundle/group[text() = \"org.apache.nifi\"]/parent::bundle/version/text()",
                            flowDocument, XPathConstants.NODESET);

            HashSet<String> versionNumbers = new HashSet<>();

            final int length = processorNodeVersion.getLength();
            for (int i=0; i<length; i++) {
                Node item = processorNodeVersion.item(i);

                String textContent = item.getTextContent();

                versionNumbers.add(textContent);
            }

            if (versionNumbers.size() == 0) {
                return null;
            }

            if (versionNumbers.size() > 1) {
                throw new RuntimeException(
                        "Multiple NiFi versions found in Flow file, this is unexpected: " + versionNumbers);
            }

            return versionNumbers.iterator().next();

        } catch (XPathExpressionException e) {
            throw new RuntimeException("Failure extracting version information from flow file: " + flowFile, e);
        }
    }


    public void start() {

        currentState.checkCanTransition(State.STARTED);

        try {
            if (!bootstrapLibDir.exists()) {
                throw new IllegalStateException("Not found: " + bootstrapLibDir);
            }



            System.setProperty("org.apache.jasper.compiler.disablejsr199", "true");
            System.setProperty("java.security.egd", "file:/dev/urandom");
            System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
            System.setProperty("java.net.preferIPv4Stack", "true");
            System.setProperty("java.awt.headless", "true");
            System.setProperty("java.protocol.handler.pkgs", "sun.net.www.protocol");

            System.setProperty("nifi.properties.file.path", nifiProperties.getAbsolutePath());
            System.setProperty("app", "NiFi");
            System.setProperty("org.apache.nifi.bootstrap.config.log.dir", "./logs");

            ClassLoader coreClassLoader = new NiFiCoreLibClassLoader(nifiHomeDir, ClassLoader.getSystemClassLoader());
            Thread.currentThread().setContextClassLoader(coreClassLoader);



            this.testNiFi = new EmbeddedNiFi(new String[0], coreClassLoader);

        } catch (Exception ex) {

            currentState = State.START_FAILED;

            throw new RuntimeException("Startup failed", ex);

        }

        currentState = State.STARTED;


    }


    public void stopAndCleanup() {
        currentState.checkCanTransition(State.STOPPED);

        try {
            testNiFi.shutdown();

            removeNiFiFilesCreatedForTemporaryInstallation(placeholderNiFiHomeDir);

        } catch (Exception e) {
            currentState = State.STOP_FAILED;

            throw new RuntimeException(e);
        }

        currentState = State.STOPPED;
    }

    private static File requireCurrentWorkingDirectoryIsCorrect() {

        File currentWorkDir = new File(System.getProperty("user.dir"));
        if (!currentWorkDir.getName().equals("nifi_testharness_nifi_home")) {

            throw new IllegalStateException(
                    "The test's working directory has to be set to nifi_testharness_nifi_home, but was: " + currentWorkDir);
        }
        return currentWorkDir;
    }

    private static File getActualNiFiHomeDir(File currentDir) {
        File[] files = currentDir.listFiles((dir, name) -> name.startsWith("nifi-"));

        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "No \"nifi-*\" directory found in temporary NiFi home directory container: " + currentDir);
        }

        if (files.length != 1) {
            throw new IllegalStateException(
                    "Multiple \"nifi-*\" directories found in temporary NiFi home directory container: " + currentDir);
        }

        return files[0];
    }

    private static void removeNiFiFilesCreatedForTemporaryInstallation(File directoryToClear) {

        if (directoryToClear != null) {
            File[] directoryContents = directoryToClear.listFiles();
            if (directoryContents != null) {
                Arrays.stream(directoryContents)
                        .filter(file -> !"NIFI_TESTHARNESS_README.txt".equals(file.getName()))
                        .forEach(TestNiFiInstance::deleteFileOrDirectoryRecursively);
            }
        }
    }

    private static void deleteFileOrDirectoryRecursively(File file) {
        if (file.isDirectory()) {
            FileUtils.deleteDirectoryRecursive(file);
        } else {
            boolean deletedSuccessfully = file.delete();
            if (!deletedSuccessfully) {
                throw new RuntimeException("Could not delete: " + file);
            }
        }
    }

    @Override
    public String toString() {
        return "NiFi test instance(" + Integer.toHexString(hashCode())
                + ") state: " + currentState + ", home: " + nifiHomeDir;
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {

        private boolean isDisposed = false;

        private File nifiBinaryZip;
        private File flowXml;
        private FlowFileEditorCallback editCallback;

        /**
         * Sets the location of the NiFi binary distribution file, from which the test instance
         * will be uncompressed and built.
         *
         * @param nifiBinaryZip
         *      the NiFi binary distribution file, from which the test instance will be built (never {@code null})
         * @return {@code this} (for method chaining)
         */
        public Builder setNiFiBinaryDistributionZip(File nifiBinaryZip) {
            if (!nifiBinaryZip.exists()) {
                throw new IllegalArgumentException("File not found: " + nifiBinaryZip);
            }

            if (nifiBinaryZip.isDirectory()) {
                throw new IllegalArgumentException("A ZIP file is expected to be specified, not a directory: "
                        + nifiBinaryZip);
            }

            this.nifiBinaryZip = nifiBinaryZip;
            return this;
        }

        /**
         * Sets the NiFi flow XML, which will be installed to the NiFi instance for testing.
         *
         * @param flowXml the NiFi flow file to install to the test instance for testing (never {@code null})
         *
         * @return {@code this} (for method chaining)
         */
        public Builder setFlowXmlToInstallForTesting(File flowXml) {
            if (!flowXml.exists()) {
                throw new IllegalArgumentException("File not found: " + flowXml);
            }

            this.flowXml = flowXml;
            return this;
        }

        /**
         * <p>
         * An <strong>optional</strong> callback to change the flow definition read from
         * {@link #setFlowXmlToInstallForTesting(File)}, before it is actually installed for testing.
         * (NOTE: The original file remains unchanged: changes are applied to a copy of it.)</p>
         *
         * <p>
         * NOTE: {@link SimpleNiFiFlowDefinitionEditor} provides various common flow definition changes
         * useful for testing.
         * </p>
         *
         * @param callback an <strong>optional</strong> callback to change the flow definition
         *
         * @return {@code this} (for method chaining)
         *
         * @see SimpleNiFiFlowDefinitionEditor
         */
        public Builder modifyFlowXmlBeforeInstalling(FlowFileEditorCallback callback) {
            this.editCallback = callback;
            return this;
        }



        public TestNiFiInstance build() {
            if (isDisposed) {
                throw new IllegalStateException("builder can only be used once");
            }
            isDisposed = true;

            return new TestNiFiInstance(nifiBinaryZip, flowXml, editCallback);
        }


    }


}
