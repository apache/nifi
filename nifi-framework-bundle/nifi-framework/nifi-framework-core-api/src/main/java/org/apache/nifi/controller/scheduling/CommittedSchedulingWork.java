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
package org.apache.nifi.controller.scheduling;

/**
 * Work made durable by a successful Process Session commit.
 *
 * @param inputFlowFiles input FlowFiles consumed
 * @param producedFlowFiles FlowFiles produced before connection fan-out
 */
public record CommittedSchedulingWork(long inputFlowFiles, long producedFlowFiles) {

    public CommittedSchedulingWork {
        if (inputFlowFiles < 0L || producedFlowFiles < 0L) {
            throw new IllegalArgumentException("Committed scheduling work values cannot be negative");
        }
    }
}
