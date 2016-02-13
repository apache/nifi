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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;

/**
 * <p>
 * Annotation that a Processor, ReportingTask, or Controller Service can use to indicate
 * that the component makes use of the {@link StateManager}. This annotation provides the
 * user with a description of what information is being stored so that the user is able to
 * understand what is shown to them and know what they are clearing should they choose to
 * clear the state. Additionally, the UI will not show any state information to users if
 * this annotation is not present.
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Stateful {
    /**
     * Provides a description of what information is being stored in the {@link StateManager}
     */
    String description();

    /**
     * Indicates the Scope(s) associated with the State that is stored and retrieved.
     */
    Scope[] scopes();
}
