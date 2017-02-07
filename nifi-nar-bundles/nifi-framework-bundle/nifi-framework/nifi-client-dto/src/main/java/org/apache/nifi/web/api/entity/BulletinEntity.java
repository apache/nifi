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
package org.apache.nifi.web.api.entity;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.ReadablePermission;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a BulletinDTO.
 */
@XmlRootElement(name = "bulletinEntity")
public class BulletinEntity extends Entity implements ReadablePermission {

    private Long id;
    private String groupId;
    private String sourceId;
    private Date timestamp;
    private String nodeAddress;
    private Boolean canRead;

    private BulletinDTO bulletin;

    /**
     * The BulletinDTO that is being serialized.
     *
     * @return The BulletinDTO object
     */
    public BulletinDTO getBulletin() {
        return bulletin;
    }

    public void setBulletin(BulletinDTO bulletin) {
        this.bulletin = bulletin;
    }

    /**
     * @return The id of the bulletin.
     */
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    /**
     * @return The group id of the source component.
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return The id of the source component.
     */
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return When this bulletin was generated.
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "When this bulletin was generated.",
            dataType = "string"
    )
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return If clustered, the address of the node from which the bulletin originated.
     */
    public String getNodeAddress() {
        return nodeAddress;
    }

    public void setNodeAddress(String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    @Override
    public Boolean getCanRead() {
        return canRead;
    }

    @Override
    public void setCanRead(Boolean canRead) {
        this.canRead = canRead;
    }
}
