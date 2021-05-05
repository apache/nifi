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
package org.apache.nifi.nar;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Represents an external source where the NAR files might be acquired from. Used by the NAR auto loader functionality
 * in order to poll an external source for new NAR files to load.
 */
public interface NarProvider {
    /**
     * Initializes the NAR Provider based on the given set of properties.
     */
    void initialize(NarProviderInitializationContext context);

    /**
     * Performs a listing of all NAR's that are available.
     *
     * @Return The result is a list of locations, where the format depends on the actual implementation.
     */
    Collection<String> listNars() throws IOException;

    /**
     * Fetches the NAR at the given location. The location should be one of the values returned by <code>listNars()</code>.
     */
    InputStream fetchNarContents(String location) throws IOException;
}
