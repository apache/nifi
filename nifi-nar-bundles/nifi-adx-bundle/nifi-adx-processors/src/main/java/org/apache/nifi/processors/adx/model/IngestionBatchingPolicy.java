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
package org.apache.nifi.processors.adx.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestionBatchingPolicy {

    @JsonProperty(value = "MaximumBatchingTimeSpan")
    private String maximumBatchingTimeSpan;
    @JsonProperty(value = "MaximumNumberOfItems")
    private int maximumNumberOfItems;
    @JsonProperty(value = "MaximumRawDataSizeMB")
    private int maximumRawDataSizeMB;

    public String getMaximumBatchingTimeSpan() {
        return maximumBatchingTimeSpan;
    }

    public void setMaximumBatchingTimeSpan(String maximumBatchingTimeSpan) {
        this.maximumBatchingTimeSpan = maximumBatchingTimeSpan;
    }

    public int getMaximumNumberOfItems() {
        return maximumNumberOfItems;
    }

    public void setMaximumNumberOfItems(int maximumNumberOfItems) {
        this.maximumNumberOfItems = maximumNumberOfItems;
    }

    public int getMaximumRawDataSizeMB() {
        return maximumRawDataSizeMB;
    }

    public void setMaximumRawDataSizeMB(int maximumRawDataSizeMB) {
        this.maximumRawDataSizeMB = maximumRawDataSizeMB;
    }
}
