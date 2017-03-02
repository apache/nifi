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
 * Marker annotation a
 * {@link org.apache.nifi.controller.ControllerService ControllerService} can
 * use to indicate a method should be called whenever the service is enabled.
 * Any method that has this annotation will be called every time a user enables
 * the service. Additionally, each time that NiFi is restarted, if NiFi is
 * configured to "auto-resume state" and the service is enabled, the method will
 * be invoked.
 * </p>
 *
 * <p>
 * Methods using this annotation must take either 0 arguments or a single
 * argument of type
 * {@link org.apache.nifi.controller.ConfigurationContext ConfigurationContext}.
 * </p>
 *
 * <p>
 * If a method with this annotation throws a Throwable, a log message and
 * bulletin will be issued for the component. In this event, the service will
 * remain in an 'ENABLING' state and will not be usable. All methods with this
 * annotation will then be called again after a delay. The service will not be
 * made available for use until all methods with this annotation have returned
 * without throwing anything.
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
 *
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface OnEnabled {

}
