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

package org.apache.nifi.components.connector;

/**
 * Trigger for initiating validation of a ConnectorNode.
 */
public interface ConnectorValidationTrigger {

    /**
     * Triggers validation of the given connector to occur asynchronously.
     * If the Connector's validation is already in progress by another thread, this method will
     * return without triggering another validation. If the Connector's validation is paused, this
     * will schedule the validation to occur once unpaused.
     *
     * @param connector the connector to validate
     */
    void triggerAsync(ConnectorNode connector);

    /**
     * Triggers validation of the given connector immediately in the current thread. This will
     * trigger validation even if other validation is currently in progress or if the Connector's
     * validation is paused.
     *
     * @param connector the connector to validate
     */
    void trigger(ConnectorNode connector);
}

