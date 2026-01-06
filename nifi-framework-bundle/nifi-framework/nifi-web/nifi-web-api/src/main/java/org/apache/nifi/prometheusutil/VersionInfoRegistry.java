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

package org.apache.nifi.prometheusutil;

import io.prometheus.client.Gauge;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionInfoRegistry extends AbstractMetricsRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(VersionInfoRegistry.class);
    private static final String DEFAULT_LABEL_STRING = "unknown";

    public VersionInfoRegistry() {
        nameToGaugeMap.put("NIFI_VERSION_INFO", Gauge.build()
            .name("nifi_version_info")
            .help("NiFi framework and environment version information.")
            .labelNames(
                "instance",
                "framework_version",
                "java_version",
                "revision",
                "build_tag",
                "build_branch",
                "os_name",
                "os_version",
                "os_architecture",
                "java_vendor"
            )
            .register(registry)
        );
    }

    public record VersionDetails(
            String frameworkVersion,
            String revision,
            String tag,
            String buildBranch,
            String javaVersion,
            String javaVendor,
            String osVersion,
            String osName,
            String osArchitecture
    ) { }

    public VersionDetails getVersionDetails() {
        String frameworkVersion = DEFAULT_LABEL_STRING;
        String revision = DEFAULT_LABEL_STRING;
        String tag = DEFAULT_LABEL_STRING;
        String buildBranch = DEFAULT_LABEL_STRING;

        // Retrieve universal system properties
        final String javaVersion = System.getProperty("java.version", DEFAULT_LABEL_STRING);
        final String javaVendor = System.getProperty("java.vendor", DEFAULT_LABEL_STRING);
        final String osVersion = System.getProperty("os.version", DEFAULT_LABEL_STRING);
        final String osName = System.getProperty("os.name", DEFAULT_LABEL_STRING);
        final String osArchitecture = System.getProperty("os.arch", DEFAULT_LABEL_STRING);

        try {
            // NiFi internal API to get build specifics
            final Bundle frameworkBundle = NarClassLoadersHolder.getInstance().getFrameworkBundle();
            if (frameworkBundle != null) {
                final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();
                frameworkVersion = frameworkDetails.getCoordinate().getVersion();
                revision = frameworkDetails.getBuildRevision();
                tag = frameworkDetails.getBuildTag();
                buildBranch = frameworkDetails.getBuildBranch();
            }
        } catch (Exception e) {
            LOGGER.debug("Could not retrieve NiFi bundle details for version info metric", e);
        }
        return new VersionDetails(frameworkVersion, revision, tag, buildBranch, javaVersion, javaVendor, osVersion, osName, osArchitecture);
    }

}
