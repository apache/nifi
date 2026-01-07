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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.List;

/**
 * A serialized representation of this class can be placed in the entity body of a response from the REST API containing the result of clearing bulletins for a component.
 */
@XmlRootElement(name = "clearBulletinsResultEntity")
public class ClearBulletinsResultEntity extends Entity {

    private int bulletinsCleared;
    private String componentId;
    private List<BulletinEntity> bulletins;

    /**
     * @return the number of bulletins that were cleared
     */
    @Schema(description = "The number of bulletins that were cleared.")
    public int getBulletinsCleared() {
        return bulletinsCleared;
    }

    public void setBulletinsCleared(int bulletinsCleared) {
        this.bulletinsCleared = bulletinsCleared;
    }

    /**
     * @return the id of the component for which bulletins were cleared
     */
    @Schema(description = "The id of the component for which bulletins were cleared.")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    /**
     * @return the current bulletins for the component after clearing
     */
    @Schema(description = "The current bulletins for the component after clearing.")
    public List<BulletinEntity> getBulletins() {
        return bulletins;
    }

    public void setBulletins(List<BulletinEntity> bulletins) {
        this.bulletins = bulletins;
    }
}
