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
package org.apache.nifi.processor;

import java.util.Collections;
import java.util.Set;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.OnUnscheduled;

/**
 * <p>
 * Provides a standard partial implementation of a {@link Processor}. This
 * implementation provides default behavior and various convenience hooks for
 * processing.</p>
 *
 * <p>
 * Implementation/Design note: This class follows the open/closed principle in a
 * fairly strict manner meaning that subclasses are free to customize behavior
 * in specifically designed points exclusively. If greater flexibility is
 * necessary then it is still possible to simply implement the {@link Processor}
 * interface.</p>
 *
 * <p>
 * Thread safe</p>
 *
 * @author none
 */
public abstract class AbstractSessionFactoryProcessor extends AbstractConfigurableComponent implements Processor {

    private String identifier;
    private ProcessorLog logger;
    private volatile boolean scheduled = false;
    private ControllerServiceLookup serviceLookup;
    private String description;

    @Override
    public final void initialize(final ProcessorInitializationContext context) {
        identifier = context.getIdentifier();
        logger = context.getLogger();
        serviceLookup = context.getControllerServiceLookup();
        init(context);

        description = getClass().getSimpleName() + "[id=" + identifier + "]";
    }

    /**
     * Returns the {@link ControllerServiceLookup} that was passed to the
     * {@link #init(ProcessorInitializationContext)} method
     *
     * @return
     */
    protected final ControllerServiceLookup getControllerServiceLookup() {
        return serviceLookup;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.emptySet();
    }

    protected final ProcessorLog getLogger() {
        return logger;
    }

    /**
     * Provides subclasses the ability to perform initialization logic
     *
     * @param context
     */
    protected void init(final ProcessorInitializationContext context) {
        // Provided for subclasses to override
    }

    /**
     * Returns <code>true</code> if the processor is scheduled to run,
     * <code>false</code> otherwise
     *
     * @return
     */
    protected final boolean isScheduled() {
        return scheduled;
    }

    @OnScheduled
    public final void updateScheduledTrue() {
        scheduled = true;
    }

    @OnUnscheduled
    public final void updateScheduledFalse() {
        scheduled = false;
    }

    @Override
    public final String getIdentifier() {
        return identifier;
    }

    @Override
    public String toString() {
        return description;
    }

}
