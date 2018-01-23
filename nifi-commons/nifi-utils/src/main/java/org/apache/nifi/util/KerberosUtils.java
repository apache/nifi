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

import java.net.InetAddress;
import java.net.UnknownHostException;


public class KerberosUtils {
    /**
     * Replace _HOST in kerberos principal name, using the FQDN
     *
     * @param principal the principal to authenticate as
     * @return the principal with replaced _HOST, if the hostname can be retrieved
     */
    public static String replaceHostname(String principal) {
        if (principal != null && principal.trim().length() > 0) {
            final String HOSTNAME_PATTERN = "_HOST";
            try {
                String hostname = InetAddress.getLocalHost().getCanonicalHostName();
                principal = principal.replace(HOSTNAME_PATTERN, hostname);
            } catch (UnknownHostException ex) {
                // ignore invalid hostname
            }
        }
        return principal;
    }
}
