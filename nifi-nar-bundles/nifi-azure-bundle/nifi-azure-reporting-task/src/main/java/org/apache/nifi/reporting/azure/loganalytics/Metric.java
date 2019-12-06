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
package org.apache.nifi.reporting.azure.loganalytics;
import com.google.gson.annotations.SerializedName;

public class Metric {
    public static final String CATEGORY_DATAFLOW = "DataFlow";
    public static final String CATEGORY_CONNECTIONS = "Connections";
    public static final String CATEGORY_PROCESSOR = "Processor";
    public static final String CATEGORY_JVM = "JvmMetrics";

    @SerializedName("Computer") String computer;
    @SerializedName("ProcessGroupId") private String processGroupId;
    @SerializedName("ProcessGroupName") private String processGroupName;
    @SerializedName("ProcessorId") private String processorId;
    @SerializedName("ProcessorName") private String processorName;
    @SerializedName("Count") private Long count;
    @SerializedName("Name") private String name;
    @SerializedName("CategoryName") private String categoryName;
    @SerializedName("Tags") private String tags;


    public Metric(String instanceId, String processGroupId, String processGroupName ) {
        this.computer = instanceId;
        this.processGroupName = processGroupName;
        this.processGroupId = processGroupId;
    }

    public void setCount(long value){
        this.count = Long.valueOf((long)value);
    }
    public void setCount(double value){
        this.count = Long.valueOf((long)value);
    }
    public void setCount(int value){
        this.count = Long.valueOf((long)value);
    }

    public Long getCount() {
        return this.count;
    }

    public String getComputer() {
        return computer;
    }
    public void setCoumputer(String computer) {
        this.computer = computer;
    }

    public String getProcessGroupId() {
        return processGroupId;
    }
    public void setProcessGroupId(String processGroupId) {
        this.processGroupId = processGroupId;
    }

    public String getProcessGroupName() {
        return processGroupName;
    }
    public void setProcessGroupName(String processGroupName) {
        this.processGroupName = processGroupName;
    }

    public String getProcessorId() {
        return processorId;
    }
    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    public String getProcessorName() {
        return processorName;
    }
    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public String getCategoryName() {
        return categoryName;
    }
    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public String getTags() {
        return tags;
    }
    public void setTags(String tags) {
        this.tags = tags;
    }
}