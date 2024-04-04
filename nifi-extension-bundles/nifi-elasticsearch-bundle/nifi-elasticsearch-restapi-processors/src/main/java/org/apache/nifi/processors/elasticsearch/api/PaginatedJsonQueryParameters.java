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

package org.apache.nifi.processors.elasticsearch.api;

public class PaginatedJsonQueryParameters extends JsonQueryParameters {
    private int pageCount = 0;
    private String scrollId = null;
    private String searchAfter = null;
    private String pitId = null;
    private String pageExpirationTimestamp = null;
    private String keepAlive;
    private String trackingRangeValue;

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(final int pageCount) {
        this.pageCount = pageCount;
    }

    public void incrementPageCount() {
        this.pageCount++;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(final String scrollId) {
        this.scrollId = scrollId;
    }

    public String getSearchAfter() {
        return searchAfter;
    }

    public void setSearchAfter(final String searchAfter) {
        this.searchAfter = searchAfter;
    }

    public String getPitId() {
        return pitId;
    }

    public void setPitId(final String pitId) {
        this.pitId = pitId;
    }

    public String getPageExpirationTimestamp() {
        return pageExpirationTimestamp;
    }

    public void setPageExpirationTimestamp(final String pageExpirationTimestamp) {
        this.pageExpirationTimestamp = pageExpirationTimestamp;
    }

    public String getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(final String keepAlive) {
        this.keepAlive = keepAlive;
    }

    public String getTrackingRangeValue() {
        return trackingRangeValue;
    }

    public void setTrackingRangeValue(String trackingRangeValue) {
        this.trackingRangeValue = trackingRangeValue;
    }
}
