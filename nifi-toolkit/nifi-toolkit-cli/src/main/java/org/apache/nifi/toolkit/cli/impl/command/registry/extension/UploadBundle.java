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
package org.apache.nifi.toolkit.cli.impl.command.registry.extension;

import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.registry.BundleVersionResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Uploads an extension bundle binary to the registry.
 */
public class UploadBundle extends AbstractNiFiRegistryCommand<BundleVersionResult> {

    public UploadBundle() {
        super("upload-bundle", BundleVersionResult.class);
    }

    @Override
    public String getDescription() {
        return "Uploads an extension bundle binary to the specified bucket in the registry.";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.EXT_BUNDLE_TYPE.createOption());
        addOption(CommandOption.EXT_BUNDLE_FILE.createOption());
        addOption(CommandOption.SKIP_SHA_256.createOption());
    }

    @Override
    public BundleVersionResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {
        final String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);

        final BundleType bundleType;
        try {
            bundleType = BundleType.fromString(getRequiredArg(properties, CommandOption.EXT_BUNDLE_TYPE));
        } catch (Exception e) {
            throw new NiFiRegistryException("Invalid bundle type, should be one of "
                    + BundleType.NIFI_NAR.toString() + " or " + BundleType.MINIFI_CPP.toString());
        }

        final File bundleFile = new File(getRequiredArg(properties, CommandOption.EXT_BUNDLE_FILE));
        final BundleVersionClient bundleVersionClient = client.getBundleVersionClient();
        final boolean skipSha256 = properties.containsKey(CommandOption.SKIP_SHA_256.getLongName());

        // calculate the sha256 unless we were told to skip it
        String sha256 = null;
        if (!skipSha256) {
            try (final InputStream inputStream = new FileInputStream(bundleFile)) {
                sha256 = Hex.encodeHexString(DigestUtils.sha256(inputStream));
            }
        }

        // upload the bundle...
        try (final InputStream bundleInputStream = new FileInputStream(bundleFile)) {
            final BundleVersion createdBundleVersion = bundleVersionClient.create(bucketId, bundleType, bundleInputStream, sha256);
            return new BundleVersionResult(getResultType(properties), createdBundleVersion);
        }
    }

}
