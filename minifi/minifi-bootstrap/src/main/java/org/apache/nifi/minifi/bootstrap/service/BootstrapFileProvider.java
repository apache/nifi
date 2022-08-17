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

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.STATUS_FILE_PID_KEY;
import static org.apache.nifi.minifi.bootstrap.SensitiveProperty.SENSITIVE_PROPERTIES;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapFileProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapFileProvider.class);

    private static final String MINIFI_PID_FILE_NAME = "minifi.pid";
    private static final String MINIFI_STATUS_FILE_NAME = "minifi.status";
    private static final String MINIFI_LOCK_FILE_NAME = "minifi.lock";
    private static final String DEFAULT_CONFIG_FILE = "./conf/bootstrap.conf";
    private static final String BOOTSTRAP_CONFIG_FILE_SYSTEM_PROPERTY_KEY = "org.apache.nifi.minifi.bootstrap.config.file";
    private static final String MINIFI_HOME_ENV_VARIABLE_KEY = "MINIFI_HOME";
    private static final String MINIFI_PID_DIR_PROP = "org.apache.nifi.minifi.bootstrap.config.pid.dir";
    private static final String DEFAULT_PID_DIR = "bin";

    private final File bootstrapConfigFile;

    public BootstrapFileProvider(File bootstrapConfigFile) {
        if (bootstrapConfigFile == null || !bootstrapConfigFile.exists()) {
            throw new IllegalArgumentException("The specified bootstrap file doesn't exists: " + bootstrapConfigFile);
        }
        this.bootstrapConfigFile = bootstrapConfigFile;
    }

    public static File getBootstrapConfFile() {
        File bootstrapConfigFile = Optional.ofNullable(System.getProperty(BOOTSTRAP_CONFIG_FILE_SYSTEM_PROPERTY_KEY))
            .map(File::new)
            .orElseGet(() -> Optional.ofNullable(System.getenv(MINIFI_HOME_ENV_VARIABLE_KEY))
                .map(File::new)
                .map(nifiHomeFile -> new File(nifiHomeFile, DEFAULT_CONFIG_FILE))
                .orElseGet(() -> new File(DEFAULT_CONFIG_FILE)));
        LOGGER.debug("Bootstrap config file: {}", bootstrapConfigFile);
        return bootstrapConfigFile;
    }

    public File getPidFile() throws IOException {
        return getBootstrapFile(MINIFI_PID_FILE_NAME);
    }

    public File getStatusFile() throws IOException {
        return getBootstrapFile(MINIFI_STATUS_FILE_NAME);
    }

    public File getLockFile() throws IOException {
        return getBootstrapFile(MINIFI_LOCK_FILE_NAME);
    }

    public File getReloadLockFile() {
        File confDir = bootstrapConfigFile.getParentFile();
        File nifiHome = confDir.getParentFile();
        File bin = new File(nifiHome, "bin");
        File reloadFile = new File(bin, "minifi.reload.lock");

        LOGGER.debug("Reload File: {}", reloadFile);
        return reloadFile;
    }

    public File getSwapFile() {
        File confDir = bootstrapConfigFile.getParentFile();
        File swapFile = new File(confDir, "swap.yml");

        LOGGER.debug("Swap File: {}", swapFile);
        return swapFile;
    }

    public Properties getBootstrapProperties() throws IOException {
        if (!bootstrapConfigFile.exists()) {
            throw new FileNotFoundException(bootstrapConfigFile.getAbsolutePath());
        }

        Properties bootstrapProperties = new Properties();
        try (FileInputStream fis = new FileInputStream(bootstrapConfigFile)) {
            bootstrapProperties.load(fis);
        }

        logProperties("Bootstrap", bootstrapProperties);

        return bootstrapProperties;
    }

    public Properties getStatusProperties() {
        Properties props = new Properties();

        try {
            File statusFile = getStatusFile();
            if (statusFile == null || !statusFile.exists()) {
                LOGGER.debug("No status file to load properties from");
                return props;
            }

            try (FileInputStream fis = new FileInputStream(statusFile)) {
                props.load(fis);
            }
        } catch (IOException exception) {
            LOGGER.error("Failed to load MiNiFi status properties");
        }

        logProperties("MiNiFi status", props);

        return props;
    }

    public synchronized void saveStatusProperties(Properties minifiProps) throws IOException {
        String pid = minifiProps.getProperty(STATUS_FILE_PID_KEY);
        if (!StringUtils.isBlank(pid)) {
            writePidFile(pid);
        }

        File statusFile = getStatusFile();
        if (statusFile.exists() && !statusFile.delete()) {
            LOGGER.warn("Failed to delete {}", statusFile);
        }

        if (!statusFile.createNewFile()) {
            throw new IOException("Failed to create file " + statusFile);
        }

        try {
            Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_WRITE);
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.OTHERS_READ);
            Files.setPosixFilePermissions(statusFile.toPath(), perms);
        } catch (Exception e) {
            LOGGER.warn("Failed to set permissions so that only the owner can read status file {}; "
                + "this may allows others to have access to the key needed to communicate with MiNiFi. "
                + "Permissions should be changed so that only the owner can read this file", statusFile);
        }

        try (FileOutputStream fos = new FileOutputStream(statusFile)) {
            minifiProps.store(fos, null);
            fos.getFD().sync();
        }

        LOGGER.debug("Saving MiNiFi properties to {}", statusFile);
        logProperties("Saved MiNiFi", minifiProps);
    }

    private void writePidFile(String pid) throws IOException {
        File pidFile = getPidFile();
        if (pidFile.exists() && !pidFile.delete()) {
            LOGGER.warn("Failed to delete {}", pidFile);
        }

        if (!pidFile.createNewFile()) {
            throw new IOException("Failed to create file " + pidFile);
        }

        try {
            Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(pidFile.toPath(), perms);
        } catch (Exception e) {
            LOGGER.warn("Failed to set permissions so that only the owner can read pid file {}; "
                + "this may allows others to have access to the key needed to communicate with MiNiFi. "
                + "Permissions should be changed so that only the owner can read this file", pidFile);
        }

        try (FileOutputStream fos = new FileOutputStream(pidFile)) {
            fos.write(pid.getBytes(StandardCharsets.UTF_8));
            fos.getFD().sync();
        }

        LOGGER.debug("Saved Pid {} to {}", pid, pidFile);
    }

    private File getBootstrapFile(String fileName) throws IOException {
        File configFileDir = Optional.ofNullable(System.getProperty(MINIFI_PID_DIR_PROP))
            .map(String::trim)
            .map(File::new)
            .orElseGet(() -> {
                File confDir = bootstrapConfigFile.getParentFile();
                File nifiHome = confDir.getParentFile();
                return new File(nifiHome, DEFAULT_PID_DIR);
            });

        FileUtils.ensureDirectoryExistAndCanAccess(configFileDir);
        File statusFile = new File(configFileDir, fileName);
        LOGGER.debug("Run File: {}", statusFile);

        return statusFile;
    }

    private void logProperties(String type, Properties props) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} properties: {}", type, props.entrySet()
                .stream()
                .filter(e -> {
                    String key = ((String) e.getKey()).toLowerCase();
                    return !SENSITIVE_PROPERTIES.contains(key);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
    }
}
