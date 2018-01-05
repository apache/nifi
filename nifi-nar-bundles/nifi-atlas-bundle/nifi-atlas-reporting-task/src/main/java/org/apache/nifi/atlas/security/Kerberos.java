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
package org.apache.nifi.atlas.security;

import org.apache.atlas.AtlasClientV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.NIFI_KERBEROS_KEYTAB;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.NIFI_KERBEROS_PRINCIPAL;

public class Kerberos implements AtlasAuthN {

    private String principal;
    private String keytab;

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return Stream.of(
                validateRequiredField(context, NIFI_KERBEROS_PRINCIPAL),
                validateRequiredField(context, NIFI_KERBEROS_KEYTAB)
        ).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    @Override
    public void populateProperties(Properties properties) {
        properties.put("atlas.authentication.method.kerberos", "true");
    }

    @Override
    public void configure(PropertyContext context) {
        principal = context.getProperty(NIFI_KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
        keytab = context.getProperty(NIFI_KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();

        if (StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("Principal is required for Kerberos auth.");
        }

        if (StringUtils.isEmpty(keytab)){
            throw new IllegalArgumentException("Keytab is required for Kerberos auth.");
        }
    }

    @Override
    public AtlasClientV2 createClient(String[] baseUrls) {
        final Configuration hadoopConf = new Configuration();
        hadoopConf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(hadoopConf);
        final UserGroupInformation ugi;
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            ugi = UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            throw new RuntimeException("Failed to login with Kerberos due to: " + e, e);
        }
        return new AtlasClientV2(ugi, null, baseUrls);
    }
}
