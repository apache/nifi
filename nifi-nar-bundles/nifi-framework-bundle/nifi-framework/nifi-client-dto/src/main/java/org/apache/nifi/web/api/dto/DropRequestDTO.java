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

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimestampAdapter;

/**
 * A request to drop the contents of a connection.
 */
@XmlType(name = "dropRequest")
public class DropRequestDTO {

    private String id;
    private String uri;

    private Date submissionTime;
    private Date lastUpdated;

    private Integer percentCompleted;
    private Boolean finished;
    private String failureReason;

    private Integer currentCount;
    private Long currentSize;
    private String current;
    private Integer originalCount;
    private Long originalSize;
    private String original;
    private Integer droppedCount;
    private Long droppedSize;
    private String dropped;

    private String state;

    /**
     * The id for this drop request.
     *
     * @return The id
     */
    @ApiModelProperty(
            value = "The id for this drop request."
    )
    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The uri for linking to this drop request in this NiFi.
     *
     * @return The uri
     */
    @ApiModelProperty(
            value = "The URI for future requests to this drop request."
    )
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * @return time the query was submitted
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(
            value = "The timestamp when the query was submitted.",
            dataType = "string"
    )
    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    /**
     * @return percent completed
     */
    @ApiModelProperty(
            value = "The current percent complete."
    )
    public Integer getPercentCompleted() {
        return percentCompleted;
    }

    public void setPercentCompleted(Integer percentCompleted) {
        this.percentCompleted = percentCompleted;
    }

    /**
     * @return whether the query has finished
     */
    @ApiModelProperty(
            value = "Whether the query has finished."
    )
    public Boolean isFinished() {
        return finished;
    }

    public void setFinished(Boolean finished) {
        this.finished = finished;
    }

    /**
     * @return the reason, if any, that this drop request failed
     */
    @ApiModelProperty(
        value = "The reason, if any, that this drop request failed."
    )
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    /**
     * @return the time this request was last updated
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(
            value = "The last time this drop request was updated.",
            dataType = "string"
    )
    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    /**
     * @return the number of flow files currently queued.
     */
    @ApiModelProperty(
            value = "The number of flow files currently queued."
    )
    public Integer getCurrentCount() {
        return currentCount;
    }

    public void setCurrentCount(Integer currentCount) {
        this.currentCount = currentCount;
    }

    /**
     * @return the size of the flow files currently queued in bytes.
     */
    @ApiModelProperty(
            value = "The size of flow files currently queued in bytes."
    )
    public Long getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(Long currentSize) {
        this.currentSize = currentSize;
    }

    /**
     * @return the count and size of the currently queued flow files.
     */
    @ApiModelProperty(
            value = "The count and size of flow files currently queued."
    )
    public String getCurrent() {
        return current;
    }

    public void setCurrent(String current) {
        this.current = current;
    }

    /**
     * @return the number of flow files to be dropped as a result of this request.
     */
    @ApiModelProperty(
            value = "The number of flow files to be dropped as a result of this request."
    )
    public Integer getOriginalCount() {
        return originalCount;
    }

    public void setOriginalCount(Integer originalCount) {
        this.originalCount = originalCount;
    }

    /**
     * @return the size of the flow files to be dropped as a result of this request in bytes.
     */
    @ApiModelProperty(
            value = "The size of flow files to be dropped as a result of this request in bytes."
    )
    public Long getOriginalSize() {
        return originalSize;
    }

    public void setOriginalSize(Long originalSize) {
        this.originalSize = originalSize;
    }

    /**
     * @return the count and size of flow files to be dropped as a result of this request.
     */
    @ApiModelProperty(
            value = "The count and size of flow files to be dropped as a result of this request."
    )
    public String getOriginal() {
        return original;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

    /**
     * @return the number of flow files that have been dropped thus far.
     */
    @ApiModelProperty(
            value = "The number of flow files that have been dropped thus far."
    )
    public Integer getDroppedCount() {
        return droppedCount;
    }

    public void setDroppedCount(Integer droppedCount) {
        this.droppedCount = droppedCount;
    }

    /**
     * @return the size of the flow files that have been dropped thus far in bytes.
     */
    @ApiModelProperty(
            value = "The size of flow files that have been dropped thus far in bytes."
    )
    public Long getDroppedSize() {
        return droppedSize;
    }

    public void setDroppedSize(Long droppedSize) {
        this.droppedSize = droppedSize;
    }

    /**
     * @return the count and size of the flow files that have been dropped thus far.
     */
    @ApiModelProperty(
            value = "The count and size of flow files that have been dropped thus far."
    )
    public String getDropped() {
        return dropped;
    }

    public void setDropped(String dropped) {
        this.dropped = dropped;
    }

    /**
     * @return the current state of the drop request.
     */
    @ApiModelProperty(
            value = "The current state of the drop request."
    )
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

}
