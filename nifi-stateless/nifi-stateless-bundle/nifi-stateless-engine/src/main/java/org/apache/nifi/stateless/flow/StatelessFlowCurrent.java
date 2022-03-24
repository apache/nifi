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
 * The StatelessFlowCurrent is responsible for facilitating the flow of data. Like a current of air, water, etc.
 * is the body that moves, the StatelessFlowCurrent is the piece of the Stateless framework that deals with movement of data
 */
public interface StatelessFlowCurrent {
    /**
     * Triggers the dataflow, starting from 'root' or 'source' components all the way through the end of the dataflow until either
     * the source components provide no data or all data that is provided is processed.
     */
    void triggerFlow();
}
