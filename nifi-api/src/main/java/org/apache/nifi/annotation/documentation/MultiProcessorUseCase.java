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

package org.apache.nifi.annotation.documentation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 *     An annotation that can be used for Processors in order to explain a specific use case that can be
 *     accomplished using this Processor in conjunction with at least one other Processor.
 *     For Processors that are able to be used for multiple use cases, the component
 *     may be annotated with multiple MultiProcessorUseCase annotations.
 * </p>
 * <p>
 *     Note that this annotation differs from {@link UseCase} in that UseCase should describe a use case that is
 *     accomplished using only the extension that is annotated. In contrast, MultiProcessorUseCase documents a use case
 *     that is accomplished by using both the Processor that is annotated as well as other Processors.
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Repeatable(MultiProcessorUseCases.class)
public @interface MultiProcessorUseCase {

    /**
     * A simple 1 (at most 2) sentence description of the use case. This should not include any extraneous details, such
     * as caveats, examples, etc. Those can be provided using the {@link #notes()} method.
     *
     * @return a simple description of the use case
     */
    String description();

    /**
     * Most of the time, 1-2 sentences is sufficient to describe a use case. Those 1-2 sentence should then be returned
     * by the {@link #description()}. In the event that the description is not sufficient, details may be provided to
     * further explain, by providing examples, caveats, etc.
     *
     * @return any important notes that pertain to the use case
     */
    String notes() default "";

    /**
     * An optional array of keywords that can be associated with the use case.
     * @return keywords associated with the use case
     */
    String[] keywords() default {};

    /**
     * An array of {@link ProcessorConfiguration}s that are necessary in order to accomplish the task described in this use case.
     * @return an array of processor configurations
     */
    ProcessorConfiguration[] configurations();

}
