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
package org.apache.nifi.provenance.lineage;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public interface ComputeLineageResult {

    /**
     * @return all nodes for the graph
     */
    List<LineageNode> getNodes();

    /**
     * @return all links for the graph
     */
    List<LineageEdge> getEdges();

    /**
     * @return the date at which this AsynchronousLineageResult will expire
     */
    Date getExpiration();

    /**
     * @return If an error occurred while computing the lineage, this will return the
     * serialized error; otherwise, returns <code>null</code>
     */
    String getError();

    /**
     * @return an integer between 0 and 100 (inclusive) that indicates what
     * percentage of completion the computation has reached
     */
    int getPercentComplete();

    /**
     * @return Indicates whether or not the lineage has finished running
     */
    boolean isFinished();

    boolean awaitCompletion(long time, TimeUnit unit) throws InterruptedException;
}
