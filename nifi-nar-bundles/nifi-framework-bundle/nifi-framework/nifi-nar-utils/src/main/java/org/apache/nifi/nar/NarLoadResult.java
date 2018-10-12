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
import org.apache.nifi.bundle.BundleDetails;

import java.util.Set;

/**
 * Holder so we can return which bundles were loaded and which were skipped.
 */
public class NarLoadResult {

    private final Set<Bundle> loadedBundles;

    private final Set<BundleDetails> skippedBundles;

    public NarLoadResult(final Set<Bundle> loadedBundles, final Set<BundleDetails> skippedBundles) {
        this.loadedBundles = loadedBundles;
        this.skippedBundles = skippedBundles;
    }

    public Set<Bundle> getLoadedBundles() {
        return loadedBundles;
    }

    public Set<BundleDetails> getSkippedBundles() {
        return skippedBundles;
    }

}
