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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

import java.util.Objects;

@XmlType(name = "asset")
public class AssetDTO {

    private String id;
    private String name;
    private String digest;
    private Boolean missingContent;

    @Schema(description = "The identifier of the asset.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Schema(description = "The name of the asset.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "The digest of the asset, will be null if the asset content is missing.")
    public String getDigest() {
        return digest;
    }

    public void setDigest(final String digest) {
        this.digest = digest;
    }

    @Schema(description = "Indicates if the content of the asset is missing.")
    public Boolean getMissingContent() {
        return missingContent;
    }

    public void setMissingContent(final Boolean missingContent) {
        this.missingContent = missingContent;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AssetDTO assetDTO = (AssetDTO) o;
        return Objects.equals(id, assetDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
