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
 * Marker annotation a {@link org.apache.nifi.processor.Processor Processor}
 * implementation can use to indicate that its operations on FlowFiles can be
 * safely repeated across process sessions. If a processor has this annotation
 * and it allows the framework to manage session commit and rollback then the
 * framework may elect to cascade a
 * {@link org.apache.nifi.processor.ProcessSession ProcessSession} given to this
 * processor's onTrigger method to the onTrigger method of another processor. It
 * can do this knowing that if something fails along a series of processors
 * using this same session that it can all be safely rolled back without any ill
 * effects on external services which could not be rolled back and thus all the
 * processes could be safely repeated (implied idempotent behavior).
 *
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface SideEffectFree {
}
