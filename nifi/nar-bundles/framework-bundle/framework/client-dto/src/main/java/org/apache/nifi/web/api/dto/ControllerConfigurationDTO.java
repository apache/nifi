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
package org.apache.nifi.web.api.dto;

import javax.xml.bind.annotation.XmlType;

/**
 * Details for the controller configuration.
 */
@XmlType(name = "config")
public class ControllerConfigurationDTO {

    private String name;
    private String comments;
    private Integer maxTimerDrivenThreadCount;
    private Integer maxEventDrivenThreadCount;

    private Long autoRefreshIntervalSeconds;
    private Boolean siteToSiteSecure;

    private Integer timeOffset;

    private String contentViewerUrl;
    private String uri;

    /**
     * The maximum number of timer driven threads this NiFi has available.
     *
     * @return The maximum number of threads
     */
    public Integer getMaxTimerDrivenThreadCount() {
        return maxTimerDrivenThreadCount;
    }

    public void setMaxTimerDrivenThreadCount(Integer maxTimerDrivenThreadCount) {
        this.maxTimerDrivenThreadCount = maxTimerDrivenThreadCount;
    }

    /**
     * The maximum number of event driven thread this NiFi has available.
     *
     * @return
     */
    public Integer getMaxEventDrivenThreadCount() {
        return maxEventDrivenThreadCount;
    }

    public void setMaxEventDrivenThreadCount(Integer maxEventDrivenThreadCount) {
        this.maxEventDrivenThreadCount = maxEventDrivenThreadCount;
    }

    /**
     * The name of this NiFi.
     *
     * @return The name
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The comments for this NiFi.
     *
     * @return
     */
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * The interval in seconds between the automatic NiFi refresh requests. This
     * value is read only.
     *
     * @return The interval in seconds
     */
    public Long getAutoRefreshIntervalSeconds() {
        return autoRefreshIntervalSeconds;
    }

    public void setAutoRefreshIntervalSeconds(Long autoRefreshIntervalSeconds) {
        this.autoRefreshIntervalSeconds = autoRefreshIntervalSeconds;
    }

    /**
     * Indicates whether or not Site-to-Site communications with this instance
     * is secure (2-way authentication). This value is read only.
     *
     * @return
     */
    public Boolean isSiteToSiteSecure() {
        return siteToSiteSecure;
    }

    public void setSiteToSiteSecure(Boolean siteToSiteSecure) {
        this.siteToSiteSecure = siteToSiteSecure;
    }

    /**
     * The time offset of the server.
     *
     * @return
     */
    public Integer getTimeOffset() {
        return timeOffset;
    }

    public void setTimeOffset(Integer timeOffset) {
        this.timeOffset = timeOffset;
    }

    /**
     * Returns the URL for the content viewer if configured.
     *
     * @return
     */
    public String getContentViewerUrl() {
        return contentViewerUrl;
    }

    public void setContentViewerUrl(String contentViewerUrl) {
        this.contentViewerUrl = contentViewerUrl;
    }

    /**
     * The URI for this NiFi controller.
     *
     * @return
     */
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
