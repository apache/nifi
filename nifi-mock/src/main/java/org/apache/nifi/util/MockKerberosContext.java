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
package org.apache.nifi.util;

import java.io.File;
import org.apache.nifi.kerberos.KerberosContext;

public class MockKerberosContext implements KerberosContext {

    private final String kerberosServicePrincipal;
    private final File kerberosServiceKeytab;
    private final File kerberosConfigFile;

    public MockKerberosContext(final File kerberosConfigFile) {
        this(null, null, kerberosConfigFile);
    }

    public MockKerberosContext(final String kerberosServicePrincipal, final File kerberosServiceKeytab, final File kerberosConfigFile) {
        this.kerberosServicePrincipal = kerberosServicePrincipal;
        this.kerberosServiceKeytab = kerberosServiceKeytab;
        this.kerberosConfigFile = kerberosConfigFile;
    }

    @Override
    public String getKerberosServicePrincipal() {
        return kerberosServicePrincipal;
    }

    @Override
    public File getKerberosServiceKeytab() {
        return kerberosServiceKeytab;
    }

    @Override
    public File getKerberosConfigurationFile() {
        return kerberosConfigFile;
    }
}
