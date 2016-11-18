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
 * <p>
 * Marks the usage of a component as restricted to users with elevated privileges.
 * </p>
 * <p>
 * A {@code Restricted} component is one that can be used to execute arbitrary unsanitized
 * code provided by the operator through the NiFi REST API/UI or can be used to obtain
 * or alter data on the NiFi host system using the NiFi OS credentials. These components
 * could be used by an otherwise authorized NiFi user to go beyond the intended use of
 * the application, escalate privilege, or could expose data about the internals of the
 * NiFi process or the host system. All of these capabilities should be considered
 * privileged, and admins should be aware of these capabilities and explicitly enable
 * them for a subset of trusted users.
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Restricted {
    /**
     * Provides a description of why the component usage is restricted
     */
    String value();
}
