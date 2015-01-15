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
package org.apache.nifi.processor.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Marker annotation a processor implementation can use to indicate a method
 * should be called whenever the processor is no longer scheduled to run.
 * Methods marked with this annotation will be invoked each time the framework
 * is notified to stop scheduling the processor. This method is invoked as other
 * threads are potentially running. To invoke a method after all threads have
 * finished processing, see the {@link OnStopped} annotation.
 * </p>
 *
 * If any method annotated with this annotation throws, the processor will not
 * be scheduled to run.
 *
 * @author none
 * @deprecated This Annotation has been replaced by the {@link org.apache.nifi.annotation.lifecycle.OnUnscheduled} annotation.
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Deprecated
public @interface OnUnscheduled {
}
