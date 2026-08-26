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
package org.apache.nifi.smb.common;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;

import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.PORT_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;

public class SmbProperties {

    public static final String OLD_HOSTNAME_PROPERTY_NAME = "hostname";
    public static final String OLD_PORT_PROPERTY_NAME = "port";
    public static final String OLD_SHARE_PROPERTY_NAME = "share";
    public static final String OLD_DOMAIN_PROPERTY_NAME = "domain";
    public static final String OLD_USERNAME_PROPERTY_NAME = "username";
    public static final String OLD_PASSWORD_PROPERTY_NAME = "password";
    public static final String OLD_SMB_DIALECT_PROPERTY_NAME = "smb-dialect";
    public static final String OLD_USE_ENCRYPTION_PROPERTY_NAME = "use-encryption";
    public static final String OLD_ENABLE_DFS_PROPERTY_NAME = "enable-dfs";
    public static final String OLD_TIMEOUT_PROPERTY_NAME = "timeout";

    public enum AuthenticationType implements DescribedValue {
        USERNAME_PASSWORD("Username / Password", "Use username and password to authenticate"),
        KERBEROS("Kerberos", "Use Kerberos to authenticate"),;

        private final String displayName;
        private final String description;

        AuthenticationType(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return displayName;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The network host of the SMB file server.")
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("Port to use for connection.")
            .required(true)
            .addValidator(PORT_VALIDATOR)
            .defaultValue("445")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .name("Share")
            .description("The network share that hosts the files. This is the \"first folder\"" +
                    "after the hostname: smb://hostname:port/[share]/dir1/dir2")
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("Authentication Type")
            .description("The authentication type.")
            .required(true)
            .allowableValues(AuthenticationType.class)
            .defaultValue(AuthenticationType.USERNAME_PASSWORD)
            .build();

    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.USERNAME_PASSWORD)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used for authentication. If no username is set then anonymous authentication is attempted.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.USERNAME_PASSWORD)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password used for authentication.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.USERNAME_PASSWORD)
            .build();

    public static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("Kerberos User Service")
            .description("The Kerberos User Controller Service used for authentication")
            .identifiesControllerService(KerberosUserService.class)
            .required(true)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.KERBEROS)
            .build();

    public static final PropertyDescriptor SMB_DIALECT = new PropertyDescriptor.Builder()
            .name("SMB Dialect")
            .description("The SMB dialect is negotiated between the client and the server by default to the highest common version supported by both end. " +
                    "In some rare cases, the client-server communication may fail with the automatically negotiated dialect. This property can be used to set the dialect explicitly " +
                    "(e.g. to downgrade to a lower version), when those situations would occur.")
            .required(true)
            .allowableValues(SmbDialect.class)
            .defaultValue(SmbDialect.AUTO.getValue())
            .build();

    public static final PropertyDescriptor USE_ENCRYPTION = new PropertyDescriptor.Builder()
            .name("Use Encryption")
            .description("Turns on/off encrypted communication between the client and the server. The property's behavior is SMB dialect dependent: " +
                    "SMB 2.x does not support encryption and the property has no effect. " +
                    "In case of SMB 3.x, it is a hint/request to the server to turn encryption on if the server also supports it.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor ENABLE_DFS = new PropertyDescriptor.Builder()
            .name("Enable DFS")
            .description("Enables accessing Distributed File System (DFS) and following DFS links during SMB operations.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout")
            .description("Timeout for read and write operations.")
            .required(true)
            .defaultValue("5 sec")
            .addValidator(TIME_PERIOD_VALIDATOR)
            .build();
}
