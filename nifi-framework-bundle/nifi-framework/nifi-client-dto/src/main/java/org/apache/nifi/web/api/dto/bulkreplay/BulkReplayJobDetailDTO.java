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
package org.apache.nifi.web.api.dto.bulkreplay;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

import java.util.List;

/**
 * Submission body for a bulk replay job. Extends the summary with the list of items to replay.
 * The {@code items} field is populated by the client on POST and is absent in GET responses
 * (items are retrieved separately via {@code GET /bulk-replay/jobs/{id}/items}).
 */
@XmlType(name = "bulkReplayJobDetail")
public class BulkReplayJobDetailDTO extends BulkReplayJobSummaryDTO {

    private List<BulkReplayJobItemDTO> items;

    @Schema(description = "Items to replay. Populated on submission; absent in responses.")
    public List<BulkReplayJobItemDTO> getItems() {
        return items;
    }

    public void setItems(List<BulkReplayJobItemDTO> items) {
        this.items = items;
    }
}
