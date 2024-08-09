/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

import java.util.Objects;

@XmlType(name = "assetReference")
public class AssetReferenceDTO {

    private String id;
    private String name;

    public AssetReferenceDTO() {
    }

    public AssetReferenceDTO(final String id) {
        this.id = id;
    }

    public AssetReferenceDTO(final String id, final String name) {
        this.id = id;
        this.name = name;
    }

    @Schema(description = "The identifier of the referenced asset.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Schema(description = "The name of the referenced asset.", accessMode = Schema.AccessMode.READ_ONLY)
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AssetReferenceDTO that = (AssetReferenceDTO) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
