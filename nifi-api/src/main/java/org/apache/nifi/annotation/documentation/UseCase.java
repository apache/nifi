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

import org.apache.nifi.annotation.behavior.InputRequirement;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 *     An annotation that can be used for extension points in order to explain a specific use case that can be
 *     accomplished using this extension. For components that are able to be used for multiple use cases, the component
 *     may be annotated with multiple UseCase annotations.
 * </p>
 * <p>
 *     Note that this annotation differs from {@link CapabilityDescription} in that CapabilityDescription should describe the
 *     general purpose of the extension point. UseCase, on the other hand, documents one very specific use case that
 *     can be accomplished. Some extension points may use only a single UseCase annotation while others may accomplish
 *     many use cases.
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Repeatable(UseCases.class)
public @interface UseCase {

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
     * Most Processors specify an InputRequirement of either {@link InputRequirement.Requirement#INPUT_REQUIRED INPUT_REQUIRED}
     * or {@link InputRequirement.Requirement#INPUT_FORBIDDEN}. However, some Processors use {@link InputRequirement.Requirement#INPUT_ALLOWED}
     * because some use cases require input while others do not. The inputRequirement here is only relevant for Processors that use
     * an InputRequirement of {@link InputRequirement.Requirement#INPUT_ALLOWED} and can indicate whether or not the Processor should have
     * input (aka incoming Connections) for this particular use case.
     *
     * @return the {@link InputRequirement} that corresponds to this use case.
     */
    InputRequirement.Requirement inputRequirement() default InputRequirement.Requirement.INPUT_ALLOWED;

    /**
     * An optional array of keywords that can be associated with the use case.
     * @return keywords associated with the use case
     */
    String[] keywords() default {};

    /**
     * A description of how to configure the extension for this particular use case.
     * @return a description of how to configure the extension for this particular use case.
     */
    String configuration() default "";
}
