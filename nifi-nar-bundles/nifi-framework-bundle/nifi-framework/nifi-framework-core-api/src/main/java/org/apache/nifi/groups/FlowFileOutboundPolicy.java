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

package org.apache.nifi.groups;

public enum FlowFileOutboundPolicy {

    /**
     * FlowFiles that are queued up to be transferred out of a ProcessGroup by an Output Port will be transferred
     * out of the Process Group as soon as they are available.
     */
    STREAM_WHEN_AVAILABLE,

    /**
     * FlowFiles that are queued up to be transferred out of a Process Group by an Output Port will remain queued until
     * all FlowFiles in the Process Group are ready to be transferred out of the group. The FlowFiles will then be transferred
     * out of the group. I.e., the FlowFiles will be batched together and transferred at the same time (not necessarily in a single
     * Process Session) but no FlowFile will be transferred until all FlowFiles in the group are ready to be transferred.
     */
    BATCH_OUTPUT;
}
