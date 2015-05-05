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
 * Methods marked with this annotation will be invoked each time the processor
 * is stopped and will be invoked only after the last thread has returned from
 * the <code>onTrigger</code> method.
 * </p>
 *
 * <p>
 * This means that the thread executing in this method will be the only thread
 * executing in any part of the Processor. However, since other threads may
 * later execute other parts of the code, member variables must still be
 * protected appropriately. However, access to multiple variables need not be
 * atomic.
 * </p>
 *
 * <p>
 * To indicate that a method should be called immediately when a processor is no
 * longer scheduled to run, see the {@link OnUnscheduled} annotation.
 * </p>
 *
 * @author none
 * @deprecated This Annotation has been replaced by the
 * {@link org.apache.nifi.annotation.lifecycle.OnStopped} annotation.
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Deprecated
public @interface OnStopped {

}
