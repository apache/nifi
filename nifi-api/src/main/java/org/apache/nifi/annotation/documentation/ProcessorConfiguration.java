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


import org.apache.nifi.processor.Processor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that can be used in conjunction with {@link MultiProcessorUseCase} in order to specify the different
 * components that are involved in a given use case.
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ProcessorConfiguration {

    /**
     * Returns the class of the Processor that is to be used in the use case, if it is provided. Either the
     * Processor Class or the Processor Class Name must be provided.
     *
     * @return the Processor's class, or <code>Processor</code> if the processor's classname is specified instead
     */
    Class<? extends Processor> processorClass() default Processor.class;

    /**
     * @return the fully qualified classname of the component
     */
    String processorClassName() default "";

    /**
     * @return an explanation of how the Processor should be configured.
     */
    String configuration();
}
