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

package org.apache.nifi.python;

import org.apache.nifi.controller.ControllerService;

public interface ControllerServiceTypeLookup {

    /**
     * Returns the class of the Controller Service definition whose name is specified.
     *
     * @param className the name of the Controller Service's interface. This may be the simple name of the class or the fully qualified name.
     * However, the fully qualified class name is recommended, in order to avoid any naming collisions.
     * @return the Class that defines the Controller Service's interface, or <code>null</code> if no Controller Service
     * interface can be found with the given name
     */
    Class<? extends ControllerService> lookup(String className);

    ControllerServiceTypeLookup EMPTY_LOOKUP = className -> null;
}
