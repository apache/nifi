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

package org.apache.nifi.components;

public enum PortFunction {
    /**
     * Port functions as a standard port, transferring FlowFiles to all outgoing connections.
     */
    STANDARD,

    /**
     * Port denotes that the invocation of the stateless flow has failed. If run using the Standard Engine, will operate as a Standard port. If running
     * using the Stateless Engine, the transaction is rolled back, and any input FlowFiles will be transferred to this Port.
     */
    FAILURE;
}
