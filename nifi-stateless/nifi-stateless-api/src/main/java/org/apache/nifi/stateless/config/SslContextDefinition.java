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

package org.apache.nifi.stateless.config;

public class SslContextDefinition {
    private String keystoreFile;
    private String keystorePass;
    private String keyPass;
    private String keystoreType;
    private String truststoreFile;
    private String truststorePass;
    private String truststoreType;

    public String getKeystoreFile() {
        return keystoreFile;
    }

    public void setKeystoreFile(final String keystoreFile) {
        this.keystoreFile = keystoreFile;
    }

    public String getKeystorePass() {
        return keystorePass;
    }

    public void setKeystorePass(final String keystorePass) {
        this.keystorePass = keystorePass;
    }

    public String getKeyPass() {
        return keyPass;
    }

    public void setKeyPass(final String keyPass) {
        this.keyPass = keyPass;
    }

    public String getKeystoreType() {
        return keystoreType;
    }

    public void setKeystoreType(final String keystoreType) {
        this.keystoreType = keystoreType;
    }

    public String getTruststoreFile() {
        return truststoreFile;
    }

    public void setTruststoreFile(final String truststoreFile) {
        this.truststoreFile = truststoreFile;
    }

    public String getTruststorePass() {
        return truststorePass;
    }

    public void setTruststorePass(final String truststorePass) {
        this.truststorePass = truststorePass;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public void setTruststoreType(final String truststoreType) {
        this.truststoreType = truststoreType;
    }
}
