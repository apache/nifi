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
package org.apache.nifi.web;

/**
 * Records a flow modification. This includes the resulting revision and the
 * user that performed the modification.
 */
public class FlowModification {

    private final Revision revision;
    private final String lastModifier;

    /**
     * Creates a new FlowModification.
     *
     * @param revision revision
     * @param lastModifier modifier
     */
    public FlowModification(Revision revision, String lastModifier) {
        this.revision = revision;
        this.lastModifier = lastModifier;
    }

    /**
     * Get the revision.
     *
     * @return the revision
     */
    public Revision getRevision() {
        return revision;
    }

    /**
     * Get the last modifier.
     *
     * @return the modifier
     */
    public String getLastModifier() {
        return lastModifier;
    }

    @Override
    public String toString() {
        return "Last Modified by '" + lastModifier + "' with Revision " + revision;
    }
}
