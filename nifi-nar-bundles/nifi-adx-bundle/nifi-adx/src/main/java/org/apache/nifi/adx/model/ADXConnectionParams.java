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
package org.apache.nifi.adx.model;

public class ADXConnectionParams {

    private String kustoAuthStrategy;
    private String appId;
    private String appKey;
    private String appTenant;
    private String kustoEngineURL;

    public String getKustoAuthStrategy() {
        return kustoAuthStrategy;
    }

    public void setKustoAuthStrategy(String kustoAuthStrategy) {
        this.kustoAuthStrategy = kustoAuthStrategy;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getAppTenant() {
        return appTenant;
    }

    public void setAppTenant(String appTenant) {
        this.appTenant = appTenant;
    }

    public String getKustoEngineURL() {
        return kustoEngineURL;
    }

    public void setKustoEngineURL(String kustoEngineURL) {
        this.kustoEngineURL = kustoEngineURL;
    }
}
