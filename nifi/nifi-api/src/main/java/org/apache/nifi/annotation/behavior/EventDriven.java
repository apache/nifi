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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Annotation that may be placed on a Processor that indicates to the framework
 * that the Processor is eligible to be scheduled to run based on the occurrence
 * of an "Event" (e.g., when a FlowFile is enqueued in an incoming Connection),
 * rather than being triggered periodically.
 * </p>
 *
 * <p>
 * This Annotation should not be used in conjunction with
 * {@link TriggerSerially} or {@link TriggerWhenEmpty}. If this Annotation is
 * used with either of these other Annotations, the Processor will not be
 * eligible to be scheduled in Event-Driven mode.
 * </p>
 *
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface EventDriven {

}
