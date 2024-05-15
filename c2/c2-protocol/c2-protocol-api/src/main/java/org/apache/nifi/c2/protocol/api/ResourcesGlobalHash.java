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

package org.apache.nifi.c2.protocol.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.util.Objects;

public class ResourcesGlobalHash implements Serializable {

    private static final long serialVersionUID = 1L;

    private String digest;
    private String hashType;

    @Schema(description = "The calculated hash digest value of asset repository")
    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    @Schema(description = "The type of the hashing algorithm used to calculate the hash digest")
    public String getHashType() {
        return hashType;
    }

    public void setHashType(String hashType) {
        this.hashType = hashType;
    }

    @Override
    public String toString() {
        return "ResourcesGlobalHash{" +
            "digest='" + digest + '\'' +
            ", hashType='" + hashType + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourcesGlobalHash that = (ResourcesGlobalHash) o;
        return Objects.equals(digest, that.digest) && Objects.equals(hashType, that.hashType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(digest, hashType);
    }
}
