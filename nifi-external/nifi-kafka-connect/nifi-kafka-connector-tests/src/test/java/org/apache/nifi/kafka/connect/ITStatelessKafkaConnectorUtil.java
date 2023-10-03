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

package org.apache.nifi.kafka.connect;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.io.CleanupMode.ALWAYS;

public class ITStatelessKafkaConnectorUtil {

    private static final String PROPERTIES_PATH = "target/classes/testing.properties";
    private static final String CONNECTOR_NAME = "testconnector";
    private static final String EXTENSIONS = "extensions";
    private static final String NAR = "nar";
    private static final String HASH_FILENAME = "nar-digest";
    private static final String NAR_UNPACKED_SUFFIX = "nar-unpacked";
    private static final String NEXUS_BASE_URL = "https://repository.apache.org/content/repositories/releases";
    private static final String PLUGIN_REPOSITORY_URL =
            NEXUS_BASE_URL + "/org/apache/nifi/nifi-kafka-connector-assembly/maven-metadata.xml";
    private static final String PLUGIN_URL_TEMPLATE =
            NEXUS_BASE_URL + "/org/apache/nifi/nifi-kafka-connector-assembly/%1$s/nifi-kafka-connector-assembly-%1$s.zip";

    // Checks if dataflow redeployment manages to automatically fix corrupted working directory structure.
    @Test
    public void testCreateDataflowThenCorruptWorkingDirectoryAndRedeployDataflow(@TempDir(cleanup = ALWAYS) File tempDir)
            throws IOException, HashFileDoesNotExistException {

        File extensionsDirectory = new File(tempDir, "extensions");
        File workingDirectory = new File(tempDir, "working");
        File narDirectoryRoot = new File(tempDir, "nardirectory");

        initializeDirectories(new File[]{extensionsDirectory, workingDirectory, narDirectoryRoot});

        String pluginVersion = getAvailableVersionOfPlugin();
        if (pluginVersion == null) {
            fail("Failed to find appropriate Stateless NiFi Plugin version in Nexus.");
        }
        File narDirectory = loadStatelessNiFiPlugin(narDirectoryRoot, pluginVersion);

        StatelessNiFiCommonConfig connectorConfiguration = prepareConnectorConfiguration(extensionsDirectory, workingDirectory, narDirectory, pluginVersion);

        // Deploy the dataflow
        StatelessKafkaConnectorUtil.createDataflow(connectorConfiguration);

        // Corrupt the unpacked nars
        File connectorWorkingDirectory = new File(workingDirectory, CONNECTOR_NAME);
        corruptUnpackedNars(connectorWorkingDirectory);

        // Redeploy the dataflow
        StatelessKafkaConnectorUtil.createDataflow(connectorConfiguration);

        checkWorkingDirectoryIntegrity(connectorWorkingDirectory);
    }

    private void initializeDirectories(File[] directories) throws IOException {
        for (File dir : directories) {
            purgeDirectory(dir);
        }
        for (File dir : directories) {
            if (!dir.mkdir()) {
                throw new IOException(String.format("Failed to create directory %s for testing.", dir.getPath()));
            }
        }
    }

    private void purgeDirectory(File directory) {
        if (directory.exists()) {
            deleteRecursively(directory);
        }
    }

    private void deleteRecursively(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            final File[] files = fileOrDirectory.listFiles();
            if (files != null) {
                for (final File file : files) {
                    deleteRecursively(file);
                }
            }
        }
        deleteOrLog(fileOrDirectory);
    }

    private void deleteOrLog(final File file) {
        final boolean deleted = file.delete();
        if (!deleted) {
            System.out.println(String.format("Failed to delete file %s.", file.getPath()));
        }
    }

    private String getAvailableVersionOfPlugin() throws IOException {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(PROPERTIES_PATH)) {
            properties.load(fis);
        }

        String version = properties.getProperty("project.version");
        String[] versionArray = version.split("\\.");
        String majorVersion = versionArray[0];
        String minorVersion = versionArray[1];
        String revision = versionArray[2].split("-")[0];

        String versionToSearchInRepository = majorVersion + "." + minorVersion + "." + revision;

        List<String> versionNumbers = getExistingVersionsFromRepository(PLUGIN_REPOSITORY_URL);
        String availableVersion = getMostRecendAvailableVersionNumber(versionNumbers, versionToSearchInRepository);

        return availableVersion;
    }

    private List<String> getExistingVersionsFromRepository(String repositoryUrl) throws IOException {
        URL urlObj = new URL(repositoryUrl);
        HttpURLConnection urlConnection = (HttpURLConnection) urlObj.openConnection();
        List<String> versions = Collections.emptyList();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()))) {
            Pattern versionPattern = Pattern.compile(".*<version>.*</version>.*");
            versions = br.lines()
                    .filter(line -> versionPattern.matcher(line).matches())
                    .map(line -> line.trim())
                    .map(line ->  line.substring(line.indexOf('>') + 1, line.lastIndexOf('<')))
                    .collect(Collectors.toList());
        }

        return versions;
    }

    private String getMostRecendAvailableVersionNumber(List<String> versionNumbers, String currentVersion) {
        List<String> versionNumbersDescending = versionNumbers.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        for (String version : versionNumbersDescending) {
            if (version.compareTo(currentVersion) <= 0) {
                return version;
            }
        }
        return null;
    }

    private File loadStatelessNiFiPlugin(File targetDirectory, String pluginVersion) throws IOException {
        String artifactUrl = String.format(PLUGIN_URL_TEMPLATE, pluginVersion);
        File targetFile = new File(targetDirectory.getPath(), "kafka-connector-assembly.zip");
        downloadArtifact(artifactUrl, targetFile);
        File narDirectory = unZipArchive(targetFile, targetFile.getParent());
        return narDirectory;
    }

    private StatelessNiFiCommonConfig prepareConnectorConfiguration(File extensionsDirectory, File workingDirectory, File narDirectory, String pluginVersion)
            throws IOException {
        Map<String, String> configParameters = new HashMap<>();
        configParameters.put("connector.class", "org.apache.nifi.kafka.connect.StatelessNiFiSinkConnector");
        configParameters.put("dataflow.timeout", "60 sec");
        configParameters.put("extensions.directory", extensionsDirectory.getPath());
        configParameters.put("flow.snapshot", getFlowAsString("src/test/resources/flows/GenerateFlowFile-Logattribute.json", pluginVersion));
        configParameters.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configParameters.put("krb5.file", "/etc/krb5.conf");
        configParameters.put("name", CONNECTOR_NAME);
        configParameters.put("nexus.url", NEXUS_BASE_URL);
        configParameters.put("task.class", "org.apache.nifi.kafka.connect.StatelessNiFiSinkTask");
        configParameters.put("topics", "testtopic");
        configParameters.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        configParameters.put("working.directory", workingDirectory.getPath());
        configParameters.put("nar.directory", narDirectory.getPath());
        return new StatelessNiFiSinkConfig(configParameters);
    }

    private void downloadArtifact(String url, File targetFile) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(targetFile)) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException(String.format("Failed to download artifact from %s to %s.", url, targetFile));
        }
    }

    private File unZipArchive(File zipArchive, String destinationPath) throws IOException {
        File root = null;
        try (ZipFile zipFile = new ZipFile(zipArchive)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                File entryDestination = new File(destinationPath,  entry.getName());
                if (root == null) {
                    root = entryDestination;
                }
                if (entry.isDirectory()) {
                    entryDestination.mkdirs();
                } else {
                    entryDestination.getParentFile().mkdirs();
                    try (InputStream in = zipFile.getInputStream(entry);
                         OutputStream out = new FileOutputStream(entryDestination)) {
                        IOUtils.copy(in, out);
                    }
                }
            }
        } catch (IOException exception) {
            exception.printStackTrace();
            throw new IOException(String.format("Failed to unzip archive %s.", zipArchive.getPath()));
        }
        return root;
    }

    private String getFlowAsString(String filePath, String componentVersion) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
        }
        return stringBuilder.toString().replaceAll("<component-version>", componentVersion);
    }

    private void corruptUnpackedNars(File rootFolder) {
        File extensionsFolder = new File(rootFolder, EXTENSIONS);
        removeHashFileFromOneUnpackedFolder(extensionsFolder);

        File narExtensionsFolder = new File(new File(rootFolder, NAR), EXTENSIONS);
        removeHashFileFromOneUnpackedFolder(narExtensionsFolder);
    }

    private void removeHashFileFromOneUnpackedFolder(File parent) {
        for (File entry : parent.listFiles()) {
            if (entry.getName().endsWith(NAR_UNPACKED_SUFFIX)) {
                removeFileFromFolder(entry, HASH_FILENAME);
                break;
            }
        }
    }

    private void removeFileFromFolder(File folder, String filename) {
        File fileToRemove = new File(folder, filename);
        if (fileToRemove.exists()) {
            fileToRemove.delete();
        }
    }

    private void checkWorkingDirectoryIntegrity(File workingDirectory) throws HashFileDoesNotExistException {
        File extensionsFolder = new File(workingDirectory, EXTENSIONS);
        checkIntegrity(extensionsFolder);

        File narExtensionsFolder = new File(new File(workingDirectory, NAR), EXTENSIONS);
        checkIntegrity(narExtensionsFolder);
    }

    private void checkIntegrity(File directory) throws HashFileDoesNotExistException {
        File[] unpackedDirs = directory.listFiles(file -> file.isDirectory() && file.getName().endsWith(NAR_UNPACKED_SUFFIX));
        if (unpackedDirs == null || unpackedDirs.length == 0) {
            return;
        }

        for (File unpackedDir : unpackedDirs) {
            File narHashFile = new File(unpackedDir, HASH_FILENAME);
            if (!narHashFile.exists()) {
                throw new HashFileDoesNotExistException(String.format("Hash file missing from %s.", unpackedDir.getPath()));
            }
        }
    }

    public static class HashFileDoesNotExistException extends Exception {
        public HashFileDoesNotExistException(String errorMessage) {
            super(errorMessage);
        }
    }

}
