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
package org.apache.nifi.services.smb;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.INTEGER_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.util.StringUtils;

@Tags({"microsoft", "samba"})
@CapabilityDescription("Provides connection pool for ListSmb processor. ")
public class SmbjConnectionPoolService extends AbstractControllerService implements SmbConnectionPoolService {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .displayName("Hostname")
            .name("hostname")
            .description("The network host to which files should be written.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .displayName("Share")
            .name("share")
            .description("The network share to which files should be written. This is the \"first folder\"" +
                    "after the hostname: \\\\hostname\\[share]\\dir1\\dir2")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .displayName("Domain")
            .name("domain")
            .description(
                    "The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .displayName("Username")
            .name("username")
            .description(
                    "The username used for authentication. If no username is set then anonymous authentication is attempted.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .displayName("Password")
            .name("password")
            .description("The password used for authentication. Required if Username is set.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .displayName("Port")
            .name("port")
            .description("Port to use for connection.")
            .required(true)
            .addValidator(INTEGER_VALIDATOR)
            .defaultValue("445")
            .build();
    private static final List<PropertyDescriptor> PROPERTIES = Collections
            .unmodifiableList(Arrays.asList(
                    HOSTNAME,
                    SHARE,
                    DOMAIN,
                    USERNAME,
                    PASSWORD,
                    PORT
            ));

    public SMBClient getSmbClient() {
        return smbClient;
    }

    public AuthenticationContext getAuthenticationContext() {
        return authenticationContext;
    }

    private final SMBClient smbClient = new SMBClient();
    private AuthenticationContext authenticationContext;
    private ConfigurationContext context;
    private String hostname;
    private Integer port;
    private String shareName;

    public String getShareName() {
        return shareName;
    }

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return port;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
        this.hostname = context.getProperty(HOSTNAME).getValue();
        this.port = context.getProperty(PORT).asInteger();
        this.shareName = context.getProperty(SHARE).getValue();
        createAuthenticationContext();
    }

    private void createAuthenticationContext() {
        final String userName = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        final String domain = context.getProperty(DOMAIN).getValue();
        if (userName != null && password != null) {
            authenticationContext = new AuthenticationContext(userName, password.toCharArray(), domain);
        } else {
            authenticationContext = AuthenticationContext.anonymous();
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String hostname = validationContext.getProperty(HOSTNAME).getValue();
        final Integer port = validationContext.getProperty(PORT).asInteger();
        final String share = validationContext.getProperty(SHARE).getValue();

        if (StringUtils.isBlank(hostname)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                            .explanation("hostname is required")
                    .build()
                    );
        }

        if (StringUtils.isBlank(share)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation("share is required")
                    .build()
            );
        }

        if (port == null || port <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation("port is invalid")
                    .build()
            );
        }

        return results;
    }

}
