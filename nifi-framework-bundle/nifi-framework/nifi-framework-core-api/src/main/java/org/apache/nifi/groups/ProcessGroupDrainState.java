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

/**
 * The state of a {@link ProcessGroupDrain}. Only a drain that is {@link #DRAINING} is active.
 */
public enum ProcessGroupDrainState {

    /**
     * The drain is active. This includes the time spent preparing the drain, while the sources are stopped and the components that are not sources are started.
     * {@link ProcessGroupDrain#isPrepared()} indicates whether the preparation has finished.
     */
    DRAINING,

    /**
     * Stopping, enabling, or starting the components of the Process Group for the drain failed.
     */
    FAILED,

    /**
     * The drain was ended. This does not mean that all data left the Process Group, because a drain may be ended while data is still queued.
     */
    ENDED
}
