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

package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;
import org.apache.nifi.web.api.dto.NarSummaryDTO;

import java.util.Objects;

@XmlType(name = "narSummaryEntity")
public class NarSummaryEntity extends Entity {

    private NarSummaryDTO narSummary;

    public NarSummaryEntity() {
    }

    public NarSummaryEntity(final NarSummaryDTO narSummary) {
        this.narSummary = narSummary;
    }

    @Schema(description = "The NAR summary")
    public NarSummaryDTO getNarSummary() {
        return narSummary;
    }

    public void setNarSummary(final NarSummaryDTO narSummary) {
        this.narSummary = narSummary;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NarSummaryEntity that = (NarSummaryEntity) o;
        return Objects.equals(narSummary, that.narSummary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(narSummary);
    }
}
