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
package org.apache.nifi.annotation.behavior;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that may be placed on a
 * {@link org.apache.nifi.components.ConfigurableComponent Component} describes how this component may impact a
 * system resource based on its configuration.
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Repeatable(SystemResourceConsiderations.class)
public @interface SystemResourceConsideration {

    String DEFAULT_DESCRIPTION = "An instance of this component can cause high usage of this system resource.  " +
            "Multiple instances or high concurrency settings may result a degradation of performance.";

    /**
     * The {@link SystemResource SystemResource} which may be affected by this component.
     */
    SystemResource resource();

    /**
     * A description of how this component and its configuration may affect system resource usage.
     */
    String description() default DEFAULT_DESCRIPTION;
}
