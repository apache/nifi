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

import org.apache.nifi.bundle.Bundle;

import java.io.File;
import java.util.Collection;
import java.util.Set;

/**
 * Responsible for loading NARs into the running application.
 */
public interface NarLoader {

    /**
     * Loads the given NARs.
     *
     * @param narFiles the collection of NAR files to load
     * @return the result of loading the NARs (i.e. bundles that were loaded, bundles that were skipped)
     */
    NarLoadResult load(Collection<File> narFiles);

    /**
     * Loads the given NARs, restricting the extension discovery to only the given extension types.
     *
     * @param narFiles the collection of NAR files to load
     * @param extensionTypes the extension types to discover
     * @return the result of loading the NARs (i.e. bundles that were loaded, bundles that were skipped)
     */
    NarLoadResult load(Collection<File> narFiles, Set<Class<?>> extensionTypes);

    /**
     * Unloads the given NARs.
     *
     * @param bundles the NARs to unload
     */
    void unload(Collection<Bundle> bundles);

    /**
     * Unloads the given NAR.
     *
     * @param bundle the NAR to unload
     */
    default void unload(Bundle bundle) {
        unload(Set.of(bundle));
    }
}
