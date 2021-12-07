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
package org.apache.nifi.stateless.flow;

/**
 * TransactionIngestStrategy controls how the source/root components(s) will be triggered during the dataflow execution
 * and how these components will ingest FlowFiles.
 *
 * <ul>
 *     <li>LAZY: triggers root component(s) once and then the downstream processors will be triggered
 *     <li>EAGER: triggers root component(s) till transaction thresholds reached (if there are any) or no more input data available, downstream processors will be triggered just after that
 * </ul>
 *
 * Default is LAZY which is the original behaviour.
 *
 * <p>Please note:
 * <ul>
 *     <li>depending on the processor's implementation, a single onTrigger() call may produce multiple FlowFiles (even in LAZY mode)
 *     <li>source components can be triggered again if a downstream processor cannot progress without more input (even in LAZY mode)
 * </ul>
 */
public enum TransactionIngestStrategy {
    LAZY,
    EAGER;

    public static TransactionIngestStrategy fromString(String input) {
        try {
            return valueOf(input.toUpperCase());
        } catch (NullPointerException | IllegalArgumentException e) {
            return LAZY;
        }
    }
}
