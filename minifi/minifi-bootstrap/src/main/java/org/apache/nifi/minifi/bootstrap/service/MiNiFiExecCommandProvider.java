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

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CONF_DIR_KEY;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

public class MiNiFiExecCommandProvider {

    private static final String DEFAULT_JAVA_CMD = "java";
    private static final String DEFAULT_LOG_DIR = "./logs";
    private static final String DEFAULT_LIB_DIR = "./lib";
    private static final String DEFAULT_CONF_DIR = "./conf";
    private static final String DEFAULT_CONFIG_FILE = DEFAULT_CONF_DIR + "/bootstrap.conf";
    private static final String WINDOWS_FILE_EXTENSION = ".exe";

    private final BootstrapFileProvider bootstrapFileProvider;

    public MiNiFiExecCommandProvider(BootstrapFileProvider bootstrapFileProvider) {
        this.bootstrapFileProvider = bootstrapFileProvider;
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
        Properties props = bootstrapFileProvider.getBootstrapProperties();
        File confDir = getFile(props.getProperty(CONF_DIR_KEY, DEFAULT_CONF_DIR).trim(), workingDir);
        File libDir = getFile(props.getProperty("lib.dir", DEFAULT_LIB_DIR).trim(), workingDir);
        String minifiLogDir = System.getProperty("org.apache.nifi.minifi.bootstrap.config.log.dir", DEFAULT_LOG_DIR).trim();

        List<String> cmd = new ArrayList<>();
        cmd.add(getJavaCommand(props));
        cmd.add("-classpath");
        cmd.add(buildClassPath(props, confDir, libDir));
        cmd.addAll(getJavaAdditionalArgs(props));
        cmd.add("-Dnifi.properties.file.path=" + getMiNiFiPropsFileName(props, confDir));
        cmd.add("-Dnifi.bootstrap.listen.port=" + listenPort);
        cmd.add("-Dapp=MiNiFi");
        cmd.add("-Dorg.apache.nifi.minifi.bootstrap.config.log.dir=" + minifiLogDir);
        cmd.add("org.apache.nifi.minifi.MiNiFi");

        return cmd;
    }

    private String getJavaCommand(Properties props) {
        String javaCmd = props.getProperty("java");
        if (javaCmd == null) {
            javaCmd = DEFAULT_JAVA_CMD;
        }
        if (javaCmd.equals(DEFAULT_JAVA_CMD)) {
            Optional.ofNullable(System.getenv("JAVA_HOME"))
                .map(javaHome -> getJavaCommandBasedOnExtension(javaHome, WINDOWS_FILE_EXTENSION)
                    .orElseGet(() -> getJavaCommandBasedOnExtension(javaHome, "").orElse(DEFAULT_JAVA_CMD)));
        }
        return javaCmd;
    }

    private Optional<String> getJavaCommandBasedOnExtension(String javaHome, String extension) {
        String javaCmd = null;
        File javaFile = new File(javaHome + File.separatorChar + "bin" + File.separatorChar + "java" + extension);
        if (javaFile.exists() && javaFile.canExecute()) {
            javaCmd = javaFile.getAbsolutePath();
        }
        return Optional.ofNullable(javaCmd);
    }

    private String buildClassPath(Properties props, File confDir, File libDir) {

        File[] libFiles = libDir.listFiles((dir, filename) -> filename.toLowerCase().endsWith(".jar"));
        if (libFiles == null || libFiles.length == 0) {
            throw new RuntimeException("Could not find lib directory at " + libDir.getAbsolutePath());
        }

        File[] confFiles = confDir.listFiles();
        if (confFiles == null || confFiles.length == 0) {
            throw new RuntimeException("Could not find conf directory at " + confDir.getAbsolutePath());
        }

        List<String> cpFiles = new ArrayList<>(confFiles.length + libFiles.length);
        cpFiles.add(confDir.getAbsolutePath());
        for (File file : libFiles) {
            cpFiles.add(file.getAbsolutePath());
        }

        StringBuilder classPathBuilder = new StringBuilder();
        for (int i = 0; i < cpFiles.size(); i++) {
            String filename = cpFiles.get(i);
            classPathBuilder.append(filename);
            if (i < cpFiles.size() - 1) {
                classPathBuilder.append(File.pathSeparatorChar);
            }
        }

        return classPathBuilder.toString();
    }

    private List<String> getJavaAdditionalArgs(Properties props) {
        List<String> javaAdditionalArgs = new ArrayList<>();
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if (key.startsWith("java.arg")) {
                javaAdditionalArgs.add(value);
            }
        }
        return javaAdditionalArgs;
    }

    private String getMiNiFiPropsFileName(Properties props, File confDir) {
        String minifiPropsFilename = props.getProperty("props.file");
        if (minifiPropsFilename == null) {
            if (confDir.exists()) {
                minifiPropsFilename = new File(confDir, "nifi.properties").getAbsolutePath();
            } else {
                minifiPropsFilename = DEFAULT_CONFIG_FILE;
            }
        }

        return minifiPropsFilename.trim();
    }

    private File getFile(String filename, File workingDir) {
        File file = new File(filename);
        if (!file.isAbsolute()) {
            file = new File(workingDir, filename);
        }
        return file;
    }
}
