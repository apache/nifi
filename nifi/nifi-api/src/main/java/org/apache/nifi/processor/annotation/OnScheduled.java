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
 * Marker annotation a processor implementation can use to indicate a method
 * should be called whenever the processor is scheduled for processing. This
 * will be called before any 'onTrigger' calls and will be called once each time
 * a processor instance is scheduled to run. Methods using this annotation must
 * take either 0 arguments or a single argument of type
 * {@link nifi.processor.SchedulingContext SchedulingContext}.
 *
 * If any method annotated with this annotation throws, the processor will not
 * be scheduled to run.
 *
 * @author none
 * @deprecated This Annotation has been replaced by the {@link org.apache.nifi.annotation.lifecycle.OnScheduled} annotation.
*/
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Deprecated
public @interface OnScheduled {
}
