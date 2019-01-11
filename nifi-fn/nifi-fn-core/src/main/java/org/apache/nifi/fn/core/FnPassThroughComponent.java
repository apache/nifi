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
package org.apache.nifi.fn.core;

import org.apache.nifi.fn.bootstrap.InMemoryFlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;

public class FnPassThroughComponent extends AbstractFnComponent implements FnComponent {
    static final Relationship RELATIONSHIP = new Relationship.Builder().name("").build();
    static final Set<Relationship> RELATIONSHIPS = Collections.singleton(RELATIONSHIP);

    private final FnConnectionContext connectionContext = new FnPassThroughConnectionContext();
    private final ComponentLog logger = new SLF4JComponentLog(this);


    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected FnConnectionContext getContext() {
        return connectionContext;
    }

    @Override
    protected ComponentLog getLogger() {
        return logger;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void enqueueAll(final Queue<FnFlowFile> list) {
        getChildren().get(RELATIONSHIP).forEach(child -> child.enqueueAll(list));
    }

    @Override
    public boolean runRecursive(final Queue<InMemoryFlowFile> queue) {
        for (final FnComponent component : getChildren().get(RELATIONSHIP)) {
            final boolean success = component.runRecursive(queue);
            if (!success) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isMaterializeContent() {
        return false;
    }
}
