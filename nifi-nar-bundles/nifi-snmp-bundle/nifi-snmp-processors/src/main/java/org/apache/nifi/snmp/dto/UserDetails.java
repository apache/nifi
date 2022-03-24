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
package org.apache.nifi.snmp.dto;

public class UserDetails {

    private String securityName;
    private String authProtocol;
    private String authPassphrase;
    private String privProtocol;
    private String privPassphrase;

    public UserDetails() {
    }

    public UserDetails(final String securityName, final String authProtocol, final String authPassphrase,
                       final String privProtocol, final String privPassphrase) {
        this.securityName = securityName;
        this.authProtocol = authProtocol;
        this.authPassphrase = authPassphrase;
        this.privProtocol = privProtocol;
        this.privPassphrase = privPassphrase;
    }

    public String getSecurityName() {
        return securityName;
    }

    public String getAuthProtocol() {
        return authProtocol;
    }

    public String getAuthPassphrase() {
        return authPassphrase;
    }

    public String getPrivProtocol() {
        return privProtocol;
    }

    public String getPrivPassphrase() {
        return privPassphrase;
    }
}
