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
package org.apache.nifi.kafka.service.api.producer;

import java.util.ArrayList;
import java.util.List;

/**
 * Controller service API container for wrapping results of publishing 1..n FlowFiles to Kafka.
 */
public class RecordSummary {

    private final List<FlowFileResult> flowFileResults = new ArrayList<>();

    public List<FlowFileResult> getFlowFileResults() {
        return flowFileResults;
    }

    public boolean isFailure() {
        return flowFileResults.stream().anyMatch(r -> !r.getExceptions().isEmpty());
    }
}
