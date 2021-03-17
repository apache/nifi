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
package org.apache.nifi.flowanalysis;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;

public abstract class AbstractFlowAnalysisRule extends AbstractConfigurableComponent implements FlowAnalysisRule {
    private String identifier;
    private String description;

    private ComponentLog logger;

    @Override
    public void initialize(FlowAnalysisRuleInitializationContext context) throws InitializationException {
        identifier = context.getIdentifier();
        description = getClass().getSimpleName() + "[id=" + identifier + "]";
        logger = context.getLogger();
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String toString() {
        return description;
    }

    /**
     * @return the logger that has been provided to the component by the
     * framework in its initialize method
     */
    protected ComponentLog getLogger() {
        return logger;
    }
}
