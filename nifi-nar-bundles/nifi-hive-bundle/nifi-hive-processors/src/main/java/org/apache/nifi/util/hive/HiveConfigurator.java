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
import org.apache.nifi.hadoop.KerberosTicketRenewer;
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

    private volatile KerberosTicketRenewer renewer;


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
            FileSystem.get(configuration);
        } catch (IOException ioe) {
            // Suppress exception as future uses of this configuration will fail
        }
    }

    public UserGroupInformation authenticate(final Configuration hiveConfig, String principal, String keyTab, long ticketRenewalPeriod, ComponentLog log) throws AuthenticationFailedException {

        UserGroupInformation ugi;
        try {
            ugi = SecurityUtil.loginKerberos(hiveConfig, principal, keyTab);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Hive failed", ioe);
        }

        // if we got here then we have a ugi so start a renewer
        if (ugi != null) {
            final String id = getClass().getSimpleName();
            renewer = SecurityUtil.startTicketRenewalThread(id, ugi, ticketRenewalPeriod, log);
        }
        return ugi;
    }

    public void stopRenewer() {
        if (renewer != null) {
            renewer.stop();
        }
    }
}
