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
package org.apache.nifi.remote.cluster;

public class AdaptedNodeInformation {

    private String hostname;
    private Integer siteToSitePort;
    private Integer siteToSiteHttpApiPort;

    private int apiPort;
    private boolean isSiteToSiteSecure;
    private int totalFlowFiles;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Integer getSiteToSitePort() {
        return siteToSitePort;
    }

    public void setSiteToSitePort(Integer siteToSitePort) {
        this.siteToSitePort = siteToSitePort;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public boolean isSiteToSiteSecure() {
        return isSiteToSiteSecure;
    }

    public void setSiteToSiteSecure(boolean isSiteToSiteSecure) {
        this.isSiteToSiteSecure = isSiteToSiteSecure;
    }

    public int getTotalFlowFiles() {
        return totalFlowFiles;
    }

    public void setTotalFlowFiles(int totalFlowFiles) {
        this.totalFlowFiles = totalFlowFiles;
    }

    public Integer getSiteToSiteHttpApiPort() {
        return siteToSiteHttpApiPort;
    }

    public void setSiteToSiteHttpApiPort(Integer siteToSiteHttpApiPort) {
        this.siteToSiteHttpApiPort = siteToSiteHttpApiPort;
    }

}
