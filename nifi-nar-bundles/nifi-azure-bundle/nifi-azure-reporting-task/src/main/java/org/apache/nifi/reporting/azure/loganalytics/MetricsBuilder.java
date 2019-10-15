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

import java.util.ArrayList;
import java.util.List;

/**
 * MetricsBuilder builds the list of metrics
 */
public class MetricsBuilder{
    private List<Metric> metrics = new ArrayList<>();

    private String computer;
    private String categoryName;
    private String processGroupId;
    private String processGroupName;
    private String processorId;
    private String processorName;
    private boolean isProcessorMetric = false;
    private String tags = null;


    public MetricsBuilder(String category, String instanceId, String processGroupId, String processGroupName) {
        this.computer = instanceId;
        this.processGroupName = processGroupName;
        this.processGroupId = processGroupId;
        this.categoryName = category;
        if (category.equals(Metric.CATEGORY_PROCESSOR)){
            isProcessorMetric = true;
        }
    }

    public MetricsBuilder(String category, String instanceId, String processGroupId, String processGroupName, String processorId, String processorName) {
        this(category, instanceId,processGroupId,processGroupName);
        this.processorId = processorId;
        this.processorName =processorName;
    }

    public MetricsBuilder setProcessorId(String processorId){
        this.processorId = processorId;
        return this;
    }

    public MetricsBuilder setProcessorName(String processorName){
        this.processorName = processorName;
        return this;
    }

    public MetricsBuilder setTags(String tags) {
        this.tags = tags;
        return this;
    }

    public MetricsBuilder metric(String metricName, long count){
        Metric metric = null;
        if(isProcessorMetric) {
            metric =  new Metric(this.computer, this.processGroupId, this.processGroupName);
            metric.setProcessorId(this.processorId);
            metric.setProcessorName(this.processorName);
        } else {
            metric = new Metric(this.computer, this.processGroupId, this.processGroupName);
        }
        metric.setCategoryName(this.categoryName);
        metric.setName(metricName);
        metric.setCount(count);
        if(this.tags != null) {
            metric.setTags(this.tags);
        }
        metrics.add(metric);
        return this;
    }

    public MetricsBuilder metric(String metricName, double count){
        Metric metric = null;
        if(isProcessorMetric) {
            metric =  new Metric(this.computer, this.processGroupId, this.processGroupName);
            metric.setProcessorId(this.processorId);
            metric.setProcessorName(this.processorName);
        } else {
            metric = new Metric(this.computer, this.processGroupId, this.processGroupName);
        }
        metric.setCategoryName(this.categoryName);
        metric.setName(metricName);
        metric.setCount(count);
        if(this.tags != null) {
            metric.setTags(this.tags);
        }
        metrics.add(metric);
        return this;
    }

    public MetricsBuilder metric(String metricName, int count) {
        Metric metric = null;
        if(isProcessorMetric) {
            metric =  new Metric(this.computer, this.processGroupId, this.processGroupName);
            metric.setProcessorId(this.processorId);
            metric.setProcessorName(this.processorName);
        } else {
            metric = new Metric(this.computer, this.processGroupId, this.processGroupName);
        }
        metric.setCategoryName(this.categoryName);
        metric.setName(metricName);
        metric.setCount(count);
        if(this.tags != null) {
            metric.setTags(this.tags);
        }
        metrics.add(metric);
        return this;
    }
    public List<Metric> build() {
        return metrics;
    }
    public List<Metric> getMetrics() {
        return this.metrics;
    }

    public void setMetrics(List<Metric> metrics) {
        this.metrics = metrics;
    }

    public String getComputer() {
        return this.computer;
    }

    public void setComputer(String Computer) {
        this.computer = Computer;
    }

    public String getCategoryName() {
        return this.categoryName;
    }

    public void setCategoryName(String CategoryName) {
        this.categoryName = CategoryName;
    }

    public String getProcessGroupId() {
        return this.processGroupId;
    }

    public void setProcessGroupId(String ProcessGroupId) {
        this.processGroupId = ProcessGroupId;
    }

    public String getProcessGroupName() {
        return this.processGroupName;
    }

    public void setProcessGroupName(String ProcessGroupName) {
        this.processGroupName = ProcessGroupName;
    }

    public String getProcessorId() {
        return this.processorId;
    }


    public String getProcessorName() {
        return this.processorName;
    }


    public boolean isIsProcessorMetric() {
        return this.isProcessorMetric;
    }

    public boolean getIsProcessorMetric() {
        return this.isProcessorMetric;
    }

    public void setIsProcessorMetric(boolean isProcessorMetric) {
        this.isProcessorMetric = isProcessorMetric;
    }

    public String getTags() {
        return this.tags;
    }


}