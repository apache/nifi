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

package org.apache.nifi.controller.queue;

/**
 * Specifies the order in which FlowFiles should be sorted when performing a listing of
 * FlowFiles via the {@link FlowFileQueue#listFlowFiles(String, SortColumn, SortDirection)}
 * method
 */
public enum SortDirection {
    /**
     * FlowFiles should be sorted such that the FlowFile with the lowest value for the Sort Column
     * should occur first in the listing.
     */
    ASCENDING,

    /**
     * FlowFiles should be sorted such that the FlowFile with the largest value for the Sort Column
     * should occur first in the listing.
     */
    DESCENDING;
}
