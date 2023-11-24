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
package org.apache.nifi.util.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosUser;

public class HiveConfigurator {

    public Collection<ValidationResult> validate(String configFiles, String principal, String keyTab, String password,
                                                 AtomicReference<ValidationResources> validationResourceHolder, ComponentLog log) {

        final Configuration hiveConfig = getConfigurationForValidation(validationResourceHolder, configFiles, log);

        return new ArrayList<>(KerberosProperties.validatePrincipalWithKeytabOrPassword(this.getClass().getSimpleName(), hiveConfig, principal, keyTab, password, log));
    }

    public Configuration getConfigurationForValidation(AtomicReference<ValidationResources> validationResourceHolder, String configFiles, ComponentLog log) {
        ValidationResources resources = validationResourceHolder.get();

        // if no resources in the holder, or if the holder has different resources loaded,
        // then load the Configuration and set the new resources in the holder
        if (resources == null || !configFiles.equals(resources.getConfigResources())) {
            log.debug("Reloading validation resources");
            resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
            validationResourceHolder.set(resources);
        }

        return resources.getConfiguration();
    }

    public HiveConf getConfigurationFromFiles(final String configFiles) {
        final HiveConf hiveConfig = new HiveConf();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hiveConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hiveConfig;
    }

    public void preload(Configuration configuration) {
        try {
            FileSystem.get(configuration).close();
            UserGroupInformation.setConfiguration(configuration);
        } catch (IOException ioe) {
            // Suppress exception as future uses of this configuration will fail
        }
    }

    /**
     * Acquires a {@link UserGroupInformation} using the given {@link Configuration} and {@link KerberosUser}.
     * @see SecurityUtil#getUgiForKerberosUser(Configuration, KerberosUser)
     * @param hiveConfig The Configuration to apply to the acquired UserGroupInformation
     * @param kerberosUser The KerberosUser to authenticate
     * @return A UserGroupInformation instance created using the Subject of the given KerberosUser
     * @throws AuthenticationFailedException if authentication fails
     */
    public UserGroupInformation authenticate(final Configuration hiveConfig, KerberosUser kerberosUser) throws AuthenticationFailedException {
        try {
            return SecurityUtil.getUgiForKerberosUser(hiveConfig, kerberosUser);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Hive failed", ioe);
        }
    }

}
