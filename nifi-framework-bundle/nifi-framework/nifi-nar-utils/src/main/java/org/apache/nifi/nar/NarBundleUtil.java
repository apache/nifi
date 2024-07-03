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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class NarBundleUtil {

    /**
     * Creates a BundleDetails from the given NAR working directory.
     *
     * @param narDirectory the directory of an exploded NAR which contains a META-INF/MANIFEST.MF
     *
     * @return the BundleDetails constructed from the information in META-INF/MANIFEST.MF
     */
    public static BundleDetails fromNarDirectory(final File narDirectory) throws IOException, IllegalStateException {
        if (narDirectory == null) {
            throw new IllegalArgumentException("NAR Directory cannot be null");
        }

        final File manifestFile = new File(narDirectory, "META-INF/MANIFEST.MF");
        try (final FileInputStream fis = new FileInputStream(manifestFile)) {
            final Manifest manifest = new Manifest(fis);
            final Attributes attributes = manifest.getMainAttributes();

            final BundleDetails.Builder builder = new BundleDetails.Builder();
            builder.workingDir(narDirectory);

            final String group = attributes.getValue(NarManifestEntry.NAR_GROUP.getEntryName());
            final String id = attributes.getValue(NarManifestEntry.NAR_ID.getEntryName());
            final String version = attributes.getValue(NarManifestEntry.NAR_VERSION.getEntryName());
            builder.coordinate(new BundleCoordinate(group, id, version));

            final String dependencyGroup = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_GROUP.getEntryName());
            final String dependencyId = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_ID.getEntryName());
            final String dependencyVersion = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_VERSION.getEntryName());
            if (!StringUtils.isBlank(dependencyId)) {
                builder.dependencyCoordinate(new BundleCoordinate(dependencyGroup, dependencyId, dependencyVersion));
            }

            builder.buildBranch(attributes.getValue(NarManifestEntry.BUILD_BRANCH.getEntryName()));
            builder.buildTag(attributes.getValue(NarManifestEntry.BUILD_TAG.getEntryName()));
            builder.buildRevision(attributes.getValue(NarManifestEntry.BUILD_REVISION.getEntryName()));
            builder.buildTimestamp(attributes.getValue(NarManifestEntry.BUILD_TIMESTAMP.getEntryName()));
            builder.buildJdk(attributes.getValue(NarManifestEntry.BUILD_JDK.getEntryName()));
            builder.builtBy(attributes.getValue(NarManifestEntry.BUILT_BY.getEntryName()));

            return builder.build();
        }
    }

}
