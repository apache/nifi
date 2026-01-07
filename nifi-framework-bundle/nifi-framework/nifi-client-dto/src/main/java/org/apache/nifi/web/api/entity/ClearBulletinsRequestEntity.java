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
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.InstantAdapter;

import java.time.Instant;

/**
 * A serialized representation of this class can be placed in the entity body of a request to clear bulletins for a component.
 */
@XmlRootElement(name = "clearBulletinsRequestEntity")
public class ClearBulletinsRequestEntity extends Entity {

    private Instant fromTimestamp;

    /**
     * @return the timestamp from which to clear bulletins (inclusive)
     */
    @XmlJavaTypeAdapter(InstantAdapter.class)
    @Schema(description = "The timestamp from which to clear bulletins (inclusive). This field is required.",
            type = "string",
            requiredMode = Schema.RequiredMode.REQUIRED
    )
    public Instant getFromTimestamp() {
        return fromTimestamp;
    }

    public void setFromTimestamp(Instant fromTimestamp) {
        this.fromTimestamp = fromTimestamp;
    }
}
