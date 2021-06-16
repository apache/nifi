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
package org.apache.nifi.toolkit.cli.impl.result.registry;

import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;

import java.io.IOException;
import java.io.PrintStream;

public class BundleVersionResult extends AbstractWritableResult<BundleVersion> {

    private BundleVersion extensionBundleVersion;

    public BundleVersionResult(final ResultType resultType, final BundleVersion extensionBundleVersion) {
        super(resultType);
        this.extensionBundleVersion = extensionBundleVersion;
    }

    @Override
    public BundleVersion getResult() {
        return extensionBundleVersion;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final String artifactId = extensionBundleVersion.getBundle().getArtifactId();
        final String groupId = extensionBundleVersion.getBundle().getGroupId();
        final String version = extensionBundleVersion.getVersionMetadata().getVersion();

        final String bundleCoordinate = groupId + "::" + artifactId + "::" + version;
        output.println(bundleCoordinate);
    }

}
