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
package org.apache.nifi.integration.processor;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;

public class BiConsumerProcessor extends AbstractProcessor {
    private BiConsumer<ProcessContext, ProcessSession> trigger;
    private volatile Set<Relationship> relationships;

    public void setTrigger(final BiConsumer<ProcessContext, ProcessSession> trigger) {
        this.trigger = trigger;
    }

    public void setRelationships(final Set<Relationship> relationships) {
        this.relationships = relationships;
    }

    @Override
    public Set<Relationship> getRelationships() {
        while (relationships == null) {
            try {
                Thread.sleep(1L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return Collections.emptySet();
            }
        }

        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (trigger == null) {
            throw new IllegalStateException("Trigger has not been initialized");
        }

        trigger.accept(context, session);
    }
}
