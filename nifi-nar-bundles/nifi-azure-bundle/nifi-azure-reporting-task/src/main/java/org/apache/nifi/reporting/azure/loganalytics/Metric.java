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

public class Metric {
    public static final String CATEGORY_DATAFLOW = "DataFlow";
    public static final String CATEGORY_CONNECTIONS = "Connections";
    public static final String CATEGORY_PROCESSOR = "Processor";
    public static final String CATEGORY_JVM = "JvmMetrics";


    public Metric(String instanceId, String processGroupId, String processGroupName ) {
        Computer = instanceId;
        ProcessGroupName = processGroupName;
        ProcessGroupId = processGroupId;
    }

    public void setCount(long value){
        this.Count = Long.valueOf((long)value);
    }
    public void setCount(double value){
        this.Count = Long.valueOf((long)value);
    }
    public void setCount(int value){
        this.Count = Long.valueOf((long)value);
    }
    public void setTags(String tags){
        this.Tags = tags;
    }

    public String Computer;
    public String ProcessGroupId;
    public String ProcessGroupName;
    public String ProcessorId;
    public String ProcessorName;
    public Long Count;
    public String Name;
    public String CategoryName;
    public String Tags;
}