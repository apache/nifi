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

package org.apache.nifi.annotation.lifecycle;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Marker Annotation that a Processor, Reporting Task, or Controller Service can use to indicate
 * that the method with this Annotation should be invoked whenever the component's configuration
 * is restored after a restart of NiFi.
 * </p>
 *
 * <p>
 * Methods with this annotation must take zero arguments.
 * </p>
 *
 * <p>
 * Whenever a new component is added to the flow, this method will be called immediately, since
 * there is no configuration to restore (in this case all configuration has already been restored,
 * since there is no configuration to restore).
 * </p>
 *
 * @since 0.5.0
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface OnConfigurationRestored {

}
