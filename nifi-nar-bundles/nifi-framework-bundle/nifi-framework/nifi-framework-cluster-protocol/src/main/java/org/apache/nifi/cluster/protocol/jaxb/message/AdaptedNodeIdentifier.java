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
package org.apache.nifi.cluster.protocol.jaxb.message;

/**
 */
public class AdaptedNodeIdentifier {

    private String id;
    private String apiAddress;
    private int apiPort;
    private String socketAddress;
    private int socketPort;
    private String siteToSiteAddress;
    private Integer siteToSitePort;
    private Integer siteToSiteHttpApiPort;

    private boolean siteToSiteSecure;

    public AdaptedNodeIdentifier() {
    }

    public String getApiAddress() {
        return apiAddress;
    }

    public void setApiAddress(String apiAddress) {
        this.apiAddress = apiAddress;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSocketAddress() {
        return socketAddress;
    }

    public void setSocketAddress(String socketAddress) {
        this.socketAddress = socketAddress;
    }

    public int getSocketPort() {
        return socketPort;
    }

    public void setSocketPort(int socketPort) {
        this.socketPort = socketPort;
    }

    public String getSiteToSiteAddress() {
        return siteToSiteAddress;
    }

    public void setSiteToSiteAddress(String siteToSiteAddress) {
        this.siteToSiteAddress = siteToSiteAddress;
    }

    public Integer getSiteToSitePort() {
        return siteToSitePort;
    }

    public void setSiteToSitePort(Integer siteToSitePort) {
        this.siteToSitePort = siteToSitePort;
    }


    public boolean isSiteToSiteSecure() {
        return siteToSiteSecure;
    }

    public void setSiteToSiteSecure(boolean siteToSiteSecure) {
        this.siteToSiteSecure = siteToSiteSecure;
    }

    public Integer getSiteToSiteHttpApiPort() {
        return siteToSiteHttpApiPort;
    }

    public void setSiteToSiteHttpApiPort(Integer siteToSiteHttpApiPort) {
        this.siteToSiteHttpApiPort = siteToSiteHttpApiPort;
    }

}
