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
import org.apache.nifi.processor.ProcessContext;

/**
 * <p>
 * Marker annotation a {@link org.apache.nifi.processor.Processor Processor},
 * {@link org.apache.nifi.controller.ControllerService ControllerService}, or
 * {@link org.apache.nifi.reporting.ReportingTask ReportingTask} implementation
 * can use to indicate a method should be called whenever the flow is being
 * shutdown. This will be called at most once for each component in a JVM
 * lifetime. It is not, however, guaranteed that this method will be called on
 * shutdown, as the service may be killed suddenly.
 * </p>
 *
 * <p>
 * Methods with this annotation are permitted to take either 0 or 1 argument. If
 * an argument is used, it must be of type {@link ConfigurationContext} if the
 * component is a ReportingTask or Controller Service, or of type
 * {@link ProcessContext} if the component is a Processor.
 * </p>
 *
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface OnShutdown {
}
