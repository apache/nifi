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
 * Marker annotation a {@link org.apache.nifi.processor.Processor Processor} or
 * {@link org.apache.nifi.reporting.ReportingTask ReportingTask} implementation
 * can use to indicate a method should be called whenever the component is
 * scheduled to run. This will be called before any call to 'onTrigger' and will
 * be called once each time a Processor or Reporting Task is scheduled to run.
 * This occurs in one of two ways: either a user clicks to schedule the
 * component to run, or NiFi is restarted with the "auto-resume state"
 * configuration set to true (the default value) and the component is already
 * running.
 * </p>
 *
 * <p>
 * Methods using this annotation must take either 0 arguments or a single
 * argument.
 * </p>
 *
 * <p>
 * If using 1 argument and the component using the annotation is a Processor,
 * that argument must be of type
 * {@link org.apache.nifi.processor.ProcessContext ProcessContext}.
 * </p>
 *
 * <p>
 * If using 1 argument and the component using the annotation is a Reporting
 * Task, that argument must be of type
 * {@link org.apache.nifi.controller.ConfigurationContext ConfigurationContext}.
 * </p>
 *
 * <p>
 * If any method annotated with this annotation throws any Throwable, the
 * framework will wait a while and then attempt to invoke the method again. This
 * will continue until the method succeeds, and the component will then be
 * scheduled to run after this method return successfully.
 * </p>
 *
 * <p><b>Implementation Guidelines:</b>
 * <ul>
 *   <li>Methods with this annotation are expected to perform very quick, short-lived tasks. If the function is
 *       expensive or long-lived, the logic should be performed in the {@code onTrigger} method instead.</li>
 *   <li>If a method with this annotation does not return (exceptionally or otherwise) within a short period
 *       of time (the duration is configurable in the properties file), the Thread may be interrupted.</li>
 *   <li>Methods that make use of this interface should honor Java's Thread interruption mechanisms and not swallow
 *       {@link InterruptedException}.</li>
 * </ul>
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface OnScheduled {
}
