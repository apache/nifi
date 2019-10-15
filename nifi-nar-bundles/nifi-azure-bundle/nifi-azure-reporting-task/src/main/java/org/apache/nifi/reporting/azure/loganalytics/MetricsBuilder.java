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

    public MetricsBuilder(String category, String instanceId, String processGroupId, String processGroupName) {
        Computer = instanceId;
        ProcessGroupName = processGroupName;
        ProcessGroupId = processGroupId;
        CategoryName = category;
        if (category.equals(Metric.CATEGORY_PROCESSOR)){
            isProcessorMetric = true;
        }
    }

    public MetricsBuilder(String category, String instanceId, String processGroupId, String processGroupName, String processorId, String processorName) {
        this(category, instanceId,processGroupId,processGroupName);
        ProcessorId = processorId;
        ProcessorName =processorName;
    }

    public MetricsBuilder setProcessorId(String processorId){
        this.ProcessorId = processorId;
        return this;
    }

    public MetricsBuilder setProcessorName(String processorName){
        this.ProcessorName = processorName;
        return this;
    }

    public MetricsBuilder setTags(String tags) {
        this.Tags = tags;
        return this;
    }

    public MetricsBuilder metric(String metricName, long count){
        Metric metric = null;
        if(isProcessorMetric) {
            metric =  new Metric(this.Computer, this.ProcessGroupId, this.ProcessGroupName);
            metric.ProcessorId = this.ProcessorId;
            metric.ProcessorName = this.ProcessorName;
        } else {
            metric = new Metric(this.Computer, this.ProcessGroupId, this.ProcessGroupName);
        }
        metric.CategoryName = this.CategoryName;
        metric.Name = metricName;
        metric.setCount(count);
        if(this.Tags != null) {
            metric.Tags = this.Tags;
        }
        metrics.add(metric);
        return this;
    }

    public MetricsBuilder metric(String metricName, double count){
        Metric metric = null;
        if(isProcessorMetric) {
            metric =  new Metric(this.Computer, this.ProcessGroupId, this.ProcessGroupName);
            metric.ProcessorId = this.ProcessorId;
            metric.ProcessorName = this.ProcessorName;
        } else {
            metric = new Metric(this.Computer, this.ProcessGroupId, this.ProcessGroupName);
        }
        metric.CategoryName = this.CategoryName;
        metric.Name = metricName;
        metric.setCount(count);
        if(this.Tags != null) {
            metric.Tags = this.Tags;
        }
        metrics.add(metric);
        return this;
    }

    public MetricsBuilder metric(String metricName, int count) {
        Metric metric = null;
        if(isProcessorMetric) {
            metric =  new Metric(this.Computer, this.ProcessGroupId, this.ProcessGroupName);
            metric.ProcessorId = this.ProcessorId;
            metric.ProcessorName = this.ProcessorName;
        } else {
            metric = new Metric(this.Computer, this.ProcessGroupId, this.ProcessGroupName);
        }
        metric.CategoryName = this.CategoryName;
        metric.Name = metricName;
        metric.setCount(count);
        if(this.Tags != null) {
            metric.Tags = this.Tags;
        }
        metrics.add(metric);
        return this;
    }
    public List<Metric> build() {
        return metrics;
    }

    public String Computer;
    public String CategoryName;
    public String ProcessGroupId;
    public String ProcessGroupName;
    public String ProcessorId;
    public String ProcessorName;
    public boolean isProcessorMetric = false;
    public String Tags = null;

}