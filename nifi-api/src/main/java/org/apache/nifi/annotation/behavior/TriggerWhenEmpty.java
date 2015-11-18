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
 * Marker annotation a {@link org.apache.nifi.processor.Processor Processor}
 * implementation can use to indicate that the Processor should still be
 * triggered even when it has no data in its work queue.
 * </p>
 *
 * <p>
 * A Processor is scheduled to be triggered based on its configured Scheduling Period
 * and Scheduling Strategy. However, when the scheduling period elapses, the Processor
 * will not be scheduled if it has no work to do. Normally, a Processor is said to have
 * work to do if one of the following circumstances is true:
 * </p>
 *
 * <ul>
 * <li>An incoming Connection has data in its queue</li>
 * <li>The Processor has no incoming Connections.</li>
 * <li>All incoming Connections are self-loops (both the source and destination of the Connection are the same Processor).
 * </ul>
 *
 * <p>
 * If the Processor needs to be triggered to run even when the above conditions are all
 * <code>false</code>, the Processor's class can be annotated with this annotation, which
 * will cause the Processor to be triggered, even if its incoming queues are empty.
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface TriggerWhenEmpty {
}
