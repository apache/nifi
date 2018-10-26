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
package org.apache.nifi.controller.kerberos;

import java.io.File;

public class KerberosConfig {
    private final String principal;
    private final File keytabLocation;
    private final File configFile;

    public KerberosConfig(final String principal, final File keytabLocation, final File kerberosConfigurationFile) {
        this.principal = principal;
        this.keytabLocation = keytabLocation;
        this.configFile = kerberosConfigurationFile;
    }

    public String getPrincipal() {
        return principal;
    }

    public File getKeytabLocation() {
        return keytabLocation;
    }

    public File getConfigFile() {
        return configFile;
    }

    public static final KerberosConfig NOT_CONFIGURED = new KerberosConfig(null, null, null);
}
