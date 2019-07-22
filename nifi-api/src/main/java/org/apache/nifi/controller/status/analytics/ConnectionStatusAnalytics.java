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
package org.apache.nifi.controller.status.analytics;

public interface ConnectionStatusAnalytics {

    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the total number of bytes in the queue.
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */
    long getTimeToBytesBackpressureMillis();

    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the number of objects in the queue.
     * @return milliseconds until backpressure is predicted to occur, based on the number of objects in the queue.
     */
    long getTimeToCountBackpressureMillis();

    /**
     * Returns the predicted total number of bytes in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */
    long getNextIntervalBytes();

    /**
     * Returns the predicted number of objects in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     * @return milliseconds until backpressure is predicted to occur, based on the number of bytes in the queue.
     */
    int getNextIntervalCount();

    String getGroupId();
    String getId();
    String getName();
    String getSourceId();
    String getSourceName();
    String getDestinationId();
    String getDestinationName();
}
