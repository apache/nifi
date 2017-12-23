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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mburgess on 5/4/16.
 */
public class HiveConfigurator {

    public Collection<ValidationResult> validate(String configFiles, String principal, String keyTab, AtomicReference<ValidationResources> validationResourceHolder, ComponentLog log) {

        final List<ValidationResult> problems = new ArrayList<>();
        ValidationResources resources = validationResourceHolder.get();

        // if no resources in the holder, or if the holder has different resources loaded,
        // then load the Configuration and set the new resources in the holder
        if (resources == null || !configFiles.equals(resources.getConfigResources())) {
            log.debug("Reloading validation resources");
            resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
            validationResourceHolder.set(resources);
        }

        final Configuration hiveConfig = resources.getConfiguration();

        problems.addAll(KerberosProperties.validatePrincipalAndKeytab(this.getClass().getSimpleName(), hiveConfig, principal, keyTab, log));

        return problems;
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
     * As of Apache NiFi 1.5.0, due to changes made to
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}, which is used by this
     * class to authenticate a principal with Kerberos, Hive controller services no longer
     * attempt relogins explicitly.  For more information, please read the documentation for
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}.
     * <p/>
     * In previous versions of NiFi, a {@link org.apache.nifi.hadoop.KerberosTicketRenewer} was started by
     * {@link HiveConfigurator#authenticate(Configuration, String, String, long)} when the Hive
     * controller service was enabled.  The use of a separate thread to explicitly relogin could cause race conditions
     * with the implicit relogin attempts made by hadoop/Hive code on a thread that references the same
     * {@link UserGroupInformation} instance.  One of these threads could leave the
     * {@link javax.security.auth.Subject} in {@link UserGroupInformation} to be cleared or in an unexpected state
     * while the other thread is attempting to use the {@link javax.security.auth.Subject}, resulting in failed
     * authentication attempts that would leave the Hive controller service in an unrecoverable state.
     *
     * @see SecurityUtil#loginKerberos(Configuration, String, String)
     */
    public UserGroupInformation authenticate(final Configuration hiveConfig, String principal, String keyTab) throws AuthenticationFailedException {
        UserGroupInformation ugi;
        try {
            ugi = SecurityUtil.loginKerberos(hiveConfig, principal, keyTab);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Hive failed", ioe);
        }
        return ugi;
    }

    /**
     * As of Apache NiFi 1.5.0, this method has been deprecated and is now a wrapper
     * method which invokes {@link HiveConfigurator#authenticate(Configuration, String, String)}. It will no longer start a
     * {@link org.apache.nifi.hadoop.KerberosTicketRenewer} to perform explicit relogins.
     *
     * @see HiveConfigurator#authenticate(Configuration, String, String)
     */
    @Deprecated
    public UserGroupInformation authenticate(final Configuration hiveConfig, String principal, String keyTab, long ticketRenewalPeriod) throws AuthenticationFailedException {
        return authenticate(hiveConfig, principal, keyTab);
    }
}
