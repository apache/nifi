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

package org.apache.nifi.minifi.bootstrap.service;

import static java.lang.String.join;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CONF_DIR_KEY;
import static org.apache.nifi.util.NiFiProperties.PROPERTIES_FILE_PATH;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.nifi.minifi.properties.BootstrapProperties;

public class MiNiFiExecCommandProvider {

    public static final String LOG_DIR = "org.apache.nifi.minifi.bootstrap.config.log.dir";
    public static final String MINIFI_BOOTSTRAP_CONF_FILE_PATH = "minifi.bootstrap.conf.file.path";
    public static final String DEFAULT_LOG_DIR = "./logs";

    public static final String APP_LOG_FILE_NAME = "org.apache.nifi.minifi.bootstrap.config.log.app.file.name";
    public static final String APP_LOG_FILE_EXTENSION = "org.apache.nifi.minifi.bootstrap.config.log.app.file.extension";
    public static final String BOOTSTRAP_LOG_FILE_NAME = "org.apache.nifi.minifi.bootstrap.config.log.bootstrap.file.name";
    public static final String BOOTSTRAP_LOG_FILE_EXTENSION = "org.apache.nifi.minifi.bootstrap.config.log.bootstrap.file.extension";
    public static final String DEFAULT_APP_LOG_FILE_NAME = "minifi-app";
    public static final String DEFAULT_BOOTSTRAP_LOG_FILE_NAME = "minifi-bootstrap";
    public static final String DEFAULT_LOG_FILE_EXTENSION = "log";

    public static final String NIFI_BOOTSTRAP_LISTEN_PORT = "nifi.bootstrap.listen.port";

    private static final String PROPERTIES_FILE_KEY = "props.file";
    private static final String LIB_DIR_KEY = "lib.dir";
    private static final String JAVA_COMMAND_KEY = "java";
    private static final String JAVA_ARG_KEY_PREFIX = "java.arg";

    private static final String DEFAULT_JAVA_CMD = "java";
    private static final String DEFAULT_LIB_DIR = "./lib";
    private static final String DEFAULT_CONF_DIR = "./conf";
    private static final String DEFAULT_MINIFI_PROPERTIES_FILE = "minifi.properties";

    private static final String WINDOWS_FILE_EXTENSION = ".exe";
    private static final String LINUX_FILE_EXTENSION = "";
    private static final String JAR_FILE_EXTENSION = ".jar";

    private static final String JAVA_HOME_ENVIRONMENT_VARIABLE = "JAVA_HOME";
    private static final String MINIFI_CLASS_NAME = "MiNiFi";
    private static final String MINIFI_FULLY_QUALIFIED_CLASS_NAME = "org.apache.nifi.minifi." + MINIFI_CLASS_NAME;
    private static final String SYSTEM_PROPERTY_TEMPLATE = "-D%s=%s";
    private static final String APP = "app";
    private static final String CLASSPATH = "-classpath";
    private static final String BIN_DIRECTORY = "bin";

    private final BootstrapFileProvider bootstrapFileProvider;

    public MiNiFiExecCommandProvider(BootstrapFileProvider bootstrapFileProvider) {
        this.bootstrapFileProvider = bootstrapFileProvider;
    }

    public static String getMiNiFiPropertiesPath(BootstrapProperties props, File confDir) {
        return ofNullable(props.getProperty(PROPERTIES_FILE_KEY))
            .orElseGet(() -> ofNullable(confDir)
                .filter(File::exists)
                .map(File::getAbsolutePath)
                .map(parent -> Path.of(parent, DEFAULT_MINIFI_PROPERTIES_FILE).toAbsolutePath().toString())
                .orElseGet(() -> Path.of(DEFAULT_CONF_DIR, DEFAULT_MINIFI_PROPERTIES_FILE).toAbsolutePath().toString()))
            .trim();
    }

    /**
     * Returns the process arguments required for the bootstrap to start the MiNiFi process.
     *
     * @param listenPort the port where the Bootstrap process is listening
     * @param workingDir working dir of the MiNiFi
     * @return the list of arguments to start the process
     * @throws IOException throws IOException if any of the configuration file read fails
     */
    public List<String> getMiNiFiExecCommand(int listenPort, File workingDir) throws IOException {
        BootstrapProperties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();

        File confDir = getFile(bootstrapProperties.getProperty(CONF_DIR_KEY, DEFAULT_CONF_DIR).trim(), workingDir);
        File libDir = getFile(bootstrapProperties.getProperty(LIB_DIR_KEY, DEFAULT_LIB_DIR).trim(), workingDir);

        String minifiLogDir = System.getProperty(LOG_DIR, DEFAULT_LOG_DIR).trim();
        String minifiAppLogFileName = System.getProperty(APP_LOG_FILE_NAME, DEFAULT_APP_LOG_FILE_NAME).trim();
        String minifiAppLogFileExtension = System.getProperty(APP_LOG_FILE_EXTENSION, DEFAULT_LOG_FILE_EXTENSION).trim();
        String minifiBootstrapLogFileName = System.getProperty(BOOTSTRAP_LOG_FILE_NAME, DEFAULT_BOOTSTRAP_LOG_FILE_NAME).trim();
        String minifiBootstrapLogFileExtension = System.getProperty(BOOTSTRAP_LOG_FILE_EXTENSION, DEFAULT_LOG_FILE_EXTENSION).trim();

        List<String> javaCommand = List.of(getJavaCommand(bootstrapProperties), CLASSPATH, buildClassPath(confDir, libDir));
        List<String> javaAdditionalArgs = getJavaAdditionalArgs(bootstrapProperties);
        List<String> systemProperties = List.of(
            systemProperty(PROPERTIES_FILE_PATH, getMiNiFiPropertiesPath(bootstrapProperties, confDir)),
            systemProperty(MINIFI_BOOTSTRAP_CONF_FILE_PATH, bootstrapFileProvider.getBootstrapFilePath()),
            systemProperty(NIFI_BOOTSTRAP_LISTEN_PORT, listenPort),
            systemProperty(APP, MINIFI_CLASS_NAME),
            systemProperty(LOG_DIR, minifiLogDir),
            systemProperty(APP_LOG_FILE_NAME, minifiAppLogFileName),
            systemProperty(APP_LOG_FILE_EXTENSION, minifiAppLogFileExtension),
            systemProperty(BOOTSTRAP_LOG_FILE_NAME, minifiBootstrapLogFileName),
            systemProperty(BOOTSTRAP_LOG_FILE_EXTENSION, minifiBootstrapLogFileExtension)
        );

        return Stream.of(javaCommand, javaAdditionalArgs, systemProperties, List.of(MINIFI_FULLY_QUALIFIED_CLASS_NAME))
            .flatMap(List::stream)
            .toList();
    }

    private File getFile(String filename, File workingDir) {
        File file = new File(filename);
        return file.isAbsolute() ? file : new File(workingDir, filename).getAbsoluteFile();
    }

    private String getJavaCommand(BootstrapProperties bootstrapProperties) {
        String javaCommand = bootstrapProperties.getProperty(JAVA_COMMAND_KEY, DEFAULT_JAVA_CMD);
        return javaCommand.equals(DEFAULT_JAVA_CMD)
            ? ofNullable(System.getenv(JAVA_HOME_ENVIRONMENT_VARIABLE))
            .flatMap(javaHome ->
                getJavaCommandBasedOnExtension(javaHome, javaCommand, WINDOWS_FILE_EXTENSION)
                    .or(() -> getJavaCommandBasedOnExtension(javaHome, javaCommand, LINUX_FILE_EXTENSION)))
            .orElse(DEFAULT_JAVA_CMD)
            : javaCommand;
    }

    private Optional<String> getJavaCommandBasedOnExtension(String javaHome, String javaCommand, String extension) {
        return Optional.of(new File(join(File.separator, javaHome, BIN_DIRECTORY, javaCommand + extension)))
            .filter(File::exists)
            .filter(File::canExecute)
            .map(File::getAbsolutePath);
    }

    private String buildClassPath(File confDir, File libDir) {
        File[] libFiles = ofNullable(libDir.listFiles((dir, filename) -> filename.toLowerCase().endsWith(JAR_FILE_EXTENSION)))
            .filter(files -> files.length > 0)
            .orElseThrow(() -> new RuntimeException("Could not find lib directory at " + libDir.getAbsolutePath()));

        ofNullable(confDir.listFiles())
            .filter(files -> files.length > 0)
            .orElseThrow(() -> new RuntimeException("Could not find conf directory at " + confDir.getAbsolutePath()));

        return Stream.concat(Stream.of(confDir), Stream.of(libFiles))
            .map(File::getAbsolutePath)
            .collect(joining(File.pathSeparator));
    }

    private List<String> getJavaAdditionalArgs(BootstrapProperties props) {
        return props.getPropertyKeys()
            .stream()
            .filter(key -> key.startsWith(JAVA_ARG_KEY_PREFIX))
            .map(props::getProperty)
            .toList();
    }

    private String systemProperty(String key, Object value) {
        return String.format(SYSTEM_PROPERTY_TEMPLATE, key, value);
    }

}