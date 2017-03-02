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

import org.apache.nifi.controller.ConfigurationContext;

/**
 * <p>
 * Marker annotation a
 * {@link org.apache.nifi.controller.ControllerService ControllerService} can
 * use to indicate a method should be called whenever the service is disabled.
 * </p>
 *
 * <p>
 * Methods using this annotation are permitted to take zero arguments or to take
 * a single argument of type {@link ConfigurationContext}. If a method with this
 * annotation throws a Throwable, a log message and bulletin will be issued for
 * the service, but the service will still be marked as Disabled. The failing
 * method will not be called again until the service is enabled and disabled again.
 * This is done in order to prevent a ControllerService from continually failing
 * in such a way that the service could not be disabled and updated without
 * restarting the instance of NiFi.
 * </p>
 *
 * <p>
 * Note that this annotation will be ignored if applied to a ReportingTask or
 * Processor. For a Controller Service, enabling and disabling are considered
 * lifecycle events, as the action makes them usable or unusable by other
 * components. However, for a Processor and a Reporting Task, these are not
 * lifecycle events but rather a mechanism to allow a component to be excluded
 * when starting or stopping a group of components.
 * </p>
 *
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface OnDisabled {

}
