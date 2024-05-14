/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.web.ResourceNotFoundException;

public class NarNotFoundException extends ResourceNotFoundException {

    private static final String MESSAGE_FORMAT = "NAR does not exist with coordinate %s";

    private final BundleCoordinate narCoordinate;

    public NarNotFoundException(final BundleCoordinate narCoordinate) {
        super(MESSAGE_FORMAT.formatted(narCoordinate));
        this.narCoordinate = narCoordinate;
    }

    public BundleCoordinate getNarCoordinate() {
        return narCoordinate;
    }
}
