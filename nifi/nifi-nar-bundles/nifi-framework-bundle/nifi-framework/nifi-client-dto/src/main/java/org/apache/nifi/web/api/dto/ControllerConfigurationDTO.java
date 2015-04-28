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

import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

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

    private Date currentTime;
    private Integer timeOffset;

    private String contentViewerUrl;
    private String uri;

    /**
     * @return maximum number of timer driven threads this NiFi has available
     */
    public Integer getMaxTimerDrivenThreadCount() {
        return maxTimerDrivenThreadCount;
    }

    public void setMaxTimerDrivenThreadCount(Integer maxTimerDrivenThreadCount) {
        this.maxTimerDrivenThreadCount = maxTimerDrivenThreadCount;
    }

    /**
     * @return maximum number of event driven thread this NiFi has available
     */
    public Integer getMaxEventDrivenThreadCount() {
        return maxEventDrivenThreadCount;
    }

    public void setMaxEventDrivenThreadCount(Integer maxEventDrivenThreadCount) {
        this.maxEventDrivenThreadCount = maxEventDrivenThreadCount;
    }

    /**
     * @return name of this NiFi
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return comments for this NiFi
     */
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return interval in seconds between the automatic NiFi refresh requests. This value is read only
     */
    public Long getAutoRefreshIntervalSeconds() {
        return autoRefreshIntervalSeconds;
    }

    public void setAutoRefreshIntervalSeconds(Long autoRefreshIntervalSeconds) {
        this.autoRefreshIntervalSeconds = autoRefreshIntervalSeconds;
    }

    /**
     * @return Indicates whether or not Site-to-Site communications with this instance is secure (2-way authentication). This value is read only
     */
    public Boolean isSiteToSiteSecure() {
        return siteToSiteSecure;
    }

    public void setSiteToSiteSecure(Boolean siteToSiteSecure) {
        this.siteToSiteSecure = siteToSiteSecure;
    }

    /**
     * @return current time on the server
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(Date currentTime) {
        this.currentTime = currentTime;
    }

    /**
     * @return time offset of the server
     */
    public Integer getTimeOffset() {
        return timeOffset;
    }

    public void setTimeOffset(Integer timeOffset) {
        this.timeOffset = timeOffset;
    }

    /**
     * @return the URL for the content viewer if configured
     */
    public String getContentViewerUrl() {
        return contentViewerUrl;
    }

    public void setContentViewerUrl(String contentViewerUrl) {
        this.contentViewerUrl = contentViewerUrl;
    }

    /**
     * @return URI for this NiFi controller
     */
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
