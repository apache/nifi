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

import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.NIFI_KERBEROS_KEYTAB;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.NIFI_KERBEROS_PRINCIPAL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.atlas.AtlasClientV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.atlas.reporting.ReportLineageToAtlas;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kerberos.KerberosCredentialsService;

public class Kerberos implements AtlasAuthN {
    private static final String ALLOW_EXPLICIT_KEYTAB = "NIFI_ALLOW_EXPLICIT_KEYTAB";

    private String principal;
    private String keytab;

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>();

        final String explicitPrincipal = context.getProperty(NIFI_KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = context.getProperty(NIFI_KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();

        final KerberosCredentialsService credentialsService = context.getProperty(ReportLineageToAtlas.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        final String resolvedPrincipal;
        final String resolvedKeytab;
        if (credentialsService == null) {
            resolvedPrincipal = explicitPrincipal;
            resolvedKeytab = explicitKeytab;
        } else {
            resolvedPrincipal = credentialsService.getPrincipal();
            resolvedKeytab = credentialsService.getKeytab();
        }

        if (resolvedPrincipal == null || resolvedKeytab == null) {
            problems.add(new ValidationResult.Builder()
                .subject("Kerberos Credentials")
                .valid(false)
                .explanation("Both the Principal and the Keytab must be specified when using Kerberos authentication, either via the explicit properties or the Kerberos Credentials Service.")
                .build());
        }

        if (credentialsService != null && (explicitPrincipal != null || explicitKeytab != null)) {
            problems.add(new ValidationResult.Builder()
                .subject("Kerberos Credentials")
                .valid(false)
                .explanation("Cannot specify both a Kerberos Credentials Service and a principal/keytab")
                .build());
        }

        final String allowExplicitKeytabVariable = System.getenv(ALLOW_EXPLICIT_KEYTAB);
        if ("false".equalsIgnoreCase(allowExplicitKeytabVariable) && (explicitPrincipal != null || explicitKeytab != null)) {
            problems.add(new ValidationResult.Builder()
                .subject("Kerberos Credentials")
                .valid(false)
                .explanation("The '" + ALLOW_EXPLICIT_KEYTAB + "' system environment variable is configured to forbid explicitly configuring principal/keytab in processors. "
                    + "The Kerberos Credentials Service should be used instead of setting the Kerberos Keytab or Kerberos Principal property.")
                .build());
        }

        return problems;
    }

    @Override
    public void populateProperties(Properties properties) {
        properties.put("atlas.authentication.method.kerberos", "true");
    }

    @Override
    public void configure(PropertyContext context) {
        final String explicitPrincipal = context.getProperty(NIFI_KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = context.getProperty(NIFI_KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();

        final KerberosCredentialsService credentialsService = context.getProperty(ReportLineageToAtlas.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        if (credentialsService == null) {
            principal = explicitPrincipal;
            keytab = explicitKeytab;
        } else {
            principal = credentialsService.getPrincipal();
            keytab = credentialsService.getKeytab();
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
