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
package org.apache.nifi.flow.resource;

import java.io.File;

/**
 * This strategy allows to replace the already acquired external resource if the available is newer, based on the modification time.
 *
 * This strategy assumes that the external source maintains the modification time in a proper manner and the local files are
 * not modified by other parties.
 */
final class ReplaceWithNewerResolutionStrategy implements ExternalResourceConflictResolutionStrategy {

    @Override
    public boolean shouldBeFetched(final File targetDirectory, final ExternalResourceDescriptor available) {
        final File file = new File(targetDirectory, available.getLocation());
        return !file.exists() || file.lastModified() < available.getLastModified();
    }
}
