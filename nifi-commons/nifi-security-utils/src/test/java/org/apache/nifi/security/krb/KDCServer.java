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
package org.apache.nifi.security.krb;

import org.apache.hadoop.minikdc.MiniKdc;

import java.io.File;
import java.util.Properties;

/**
 * Wrapper around MiniKdc.
 */
public class KDCServer {

    private final File baseDir;
    private final Properties kdcProperties;

    private MiniKdc kdc;

    public KDCServer(final File baseDir) {
        this.baseDir = baseDir;

        this.kdcProperties = MiniKdc.createConf();
        this.kdcProperties.setProperty(MiniKdc.INSTANCE, "DefaultKrbServer");
        this.kdcProperties.setProperty(MiniKdc.ORG_NAME, "NIFI");
        this.kdcProperties.setProperty(MiniKdc.ORG_DOMAIN, "COM");
    }

    public void setMaxTicketLifetime(final String lifetimeSeconds) {
        this.kdcProperties.setProperty(MiniKdc.MAX_TICKET_LIFETIME, lifetimeSeconds);
    }

    public void setProperty(final String key, final String value) {
        this.kdcProperties.setProperty(key, value);
    }

    public synchronized void start() throws Exception {
        if (kdc == null) {
            kdc = new MiniKdc(kdcProperties, baseDir);
        }

        kdc.start();
        System.setProperty("java.security.krb5.conf", kdc.getKrb5conf().getAbsolutePath());
    }

    public synchronized void stop() {
        if (kdc != null) {
            kdc.stop();
        }
    }

    public String getRealm() {
        return kdc.getRealm();
    }

    public void createKeytabPrincipal(final File keytabFile, final String... names) throws Exception {
        kdc.createPrincipal(keytabFile, names);
    }

    public void createPasswordPrincipal(final String principal, final String password) throws Exception {
        kdc.createPrincipal(principal, password);
    }
}
