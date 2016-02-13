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
package org.apache.nifi.cluster.firewall.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file-based implementation of the ClusterFirewall interface. The class is configured with a file. If the file is empty, then everything is permissible. Otherwise, the file should contain hostnames
 * or IPs formatted as dotted decimals with an optional CIDR suffix. Each entry must be separated by a newline. An example configuration is given below:
 *
 * <code>
 * # hash character is a comment delimiter
 * 1.2.3.4         # exact IP
 * some.host.name  # a host name
 * 4.5.6.7/8       # range of CIDR IPs
 * 9.10.11.12/13   # a smaller range of CIDR IPs
 * </code>
 *
 * This class allows for synchronization with an optionally configured restore directory. If configured, then at startup, if the either the config file or the restore directory's copy is missing, then
 * the configuration file will be copied to the appropriate location. If both restore directory contains a copy that is different in content to configuration file, then an exception is thrown at
 * construction time.
 */
public class FileBasedClusterNodeFirewall implements ClusterNodeFirewall {

    private final File config;

    private final File restoreDirectory;

    private final Collection<SubnetUtils.SubnetInfo> subnetInfos = new ArrayList<>();

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(FileBasedClusterNodeFirewall.class));

    public FileBasedClusterNodeFirewall(final File config) throws IOException {
        this(config, null);
    }

    public FileBasedClusterNodeFirewall(final File config, final File restoreDirectory) throws IOException {

        if (config == null) {
            throw new IllegalArgumentException("Firewall configuration file may not be null.");
        }

        this.config = config;
        this.restoreDirectory = restoreDirectory;

        if (restoreDirectory != null) {
            // synchronize with restore directory
            try {
                syncWithRestoreDirectory();
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        if (!config.exists() && !config.createNewFile()) {
            throw new IOException("Firewall configuration file did not exist and could not be created: " + config.getAbsolutePath());
        }

        logger.info("Loading cluster firewall configuration.");
        parseConfig(config);
        logger.info("Cluster firewall configuration loaded.");
    }

    @Override
    public boolean isPermissible(final String hostOrIp) {
        try {

            // if no rules, then permit everything
            if (subnetInfos.isEmpty()) {
                return true;
            }

            final String ip;
            try {
                ip = InetAddress.getByName(hostOrIp).getHostAddress();
            } catch (final UnknownHostException uhe) {
                logger.warn("Blocking unknown host '{}'", hostOrIp, uhe);
                return false;
            }

            // check each subnet to see if IP is in range
            for (final SubnetUtils.SubnetInfo subnetInfo : subnetInfos) {
                if (subnetInfo.isInRange(ip)) {
                    return true;
                }
            }

            // no match
            logger.debug("Blocking host '{}' because it does not match our allowed list.", hostOrIp);
            return false;

        } catch (final IllegalArgumentException iae) {
            logger.debug("Blocking requested host, '{}', because it is malformed.", hostOrIp, iae);
            return false;
        }
    }

    private void syncWithRestoreDirectory() throws IOException {

        // sanity check that restore directory is a directory, creating it if necessary
        FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

        // check that restore directory is not the same as the primary directory
        if (config.getParentFile().getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
            throw new IllegalStateException(
                    String.format("Cluster firewall configuration file '%s' cannot be in the restore directory '%s' ",
                            config.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
        }

        // the restore copy will have same file name, but reside in a different directory
        final File restoreFile = new File(restoreDirectory, config.getName());

        // sync the primary copy with the restore copy
        FileUtils.syncWithRestore(config, restoreFile, logger);

    }

    private void parseConfig(final File config) throws IOException {

        // clear old information
        subnetInfos.clear();
        try (BufferedReader br = new BufferedReader(new FileReader(config))) {

            String ipOrHostLine;
            String ipCidr;
            int totalIpsAdded = 0;
            while ((ipOrHostLine = br.readLine()) != null) {

                // cleanup whitespace
                ipOrHostLine = ipOrHostLine.trim();

                if (ipOrHostLine.isEmpty() || ipOrHostLine.startsWith("#")) {
                    // skip empty lines or comments
                    continue;
                } else if (ipOrHostLine.contains("#")) {
                    // parse out comments in IP containing lines
                    ipOrHostLine = ipOrHostLine.substring(0, ipOrHostLine.indexOf("#")).trim();
                }

                // if given a complete IP, then covert to CIDR
                if (ipOrHostLine.contains("/")) {
                    ipCidr = ipOrHostLine;
                } else if (ipOrHostLine.contains("\\")) {
                    logger.warn("CIDR IP notation uses forward slashes '/'.  Replacing backslash '\\' with forward slash'/' for '{}'", ipOrHostLine);
                    ipCidr = ipOrHostLine.replace("\\", "/");
                } else {
                    try {
                        ipCidr = InetAddress.getByName(ipOrHostLine).getHostAddress();
                        if (!ipOrHostLine.equals(ipCidr)) {
                            logger.debug("Resolved host '{}' to ip '{}'", ipOrHostLine, ipCidr);
                        }
                        ipCidr += "/32";
                        logger.debug("Adding CIDR to exact IP: '{}'", ipCidr);
                    } catch (final UnknownHostException uhe) {
                        logger.warn("Firewall is skipping unknown host address: '{}'", ipOrHostLine);
                        continue;
                    }
                }

                try {
                    logger.debug("Adding CIDR IP to firewall: '{}'", ipCidr);
                    final SubnetUtils subnetUtils = new SubnetUtils(ipCidr);
                    subnetUtils.setInclusiveHostCount(true);
                    subnetInfos.add(subnetUtils.getInfo());
                    totalIpsAdded++;
                } catch (final IllegalArgumentException iae) {
                    logger.warn("Firewall is skipping invalid CIDR address: '{}'", ipOrHostLine);
                }

            }

            if (totalIpsAdded == 0) {
                logger.info("No IPs added to firewall.  Firewall will accept all requests.");
            } else {
                logger.info("Added {} IP(s) to firewall.  Only requests originating from the configured IPs will be accepted.", totalIpsAdded);
            }

        }
    }
}
