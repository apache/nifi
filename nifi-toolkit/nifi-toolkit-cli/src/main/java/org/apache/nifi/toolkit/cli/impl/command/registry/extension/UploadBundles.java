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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class UploadBundles extends AbstractNiFiRegistryCommand<StringResult> {

    public UploadBundles() {
        super("upload-bundles", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Performs a bulk upload of multiple bundles to the specified bucket in the registry. This command will look for " +
                "files in the specified directory, and if recurse (-r) is specified then it will search child directories recursively. " +
                "If fileExtension is specified then it will only consider files that have the specified extension, such as '.nar'";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.EXT_BUNDLE_TYPE.createOption());
        addOption(CommandOption.EXT_BUNDLE_DIR.createOption());
        addOption(CommandOption.FILE_EXTENSION.createOption());
        addOption(CommandOption.RECURSIVE.createOption());
        addOption(CommandOption.SKIP_SHA_256.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiRegistryClient client, final Properties properties)
            throws IOException, NiFiRegistryException, ParseException {
        final String bucketId = getRequiredArg(properties, CommandOption.BUCKET_ID);
        final String bundleDir = getRequiredArg(properties, CommandOption.EXT_BUNDLE_DIR);
        final String fileExtension = getArg(properties, CommandOption.FILE_EXTENSION);
        final boolean recursive = properties.containsKey(CommandOption.RECURSIVE);
        final boolean skipSha256 = properties.containsKey(CommandOption.SKIP_SHA_256.getLongName());
        final boolean verbose = isVerbose(properties);

        final BundleType bundleType;
        try {
            bundleType = BundleType.fromString(getRequiredArg(properties, CommandOption.EXT_BUNDLE_TYPE));
        } catch (IllegalArgumentException e) {
            throw new NiFiRegistryException("Invalid bundle type, should be one of "
                    + BundleType.NIFI_NAR.toString() + " or " + BundleType.MINIFI_CPP.toString());
        }

        final BundleVersionClient bundleVersionClient = client.getBundleVersionClient();

        final File startDir = new File(bundleDir);
        if (!startDir.exists()) {
            throw new NiFiRegistryException("The specified directory does not exist: " + startDir.getAbsolutePath());
        }
        if (!startDir.isDirectory()) {
            throw new NiFiRegistryException("The specified directory is not a directory: " + startDir.getAbsolutePath());
        }

        final AtomicInteger counter = new AtomicInteger(0);
        uploadBundle(bundleVersionClient, bucketId, bundleType, startDir, fileExtension, recursive, skipSha256, verbose, counter);

        return new StringResult("Uploaded " + counter.get() + " bundles successfully", getContext().isInteractive());
    }

    private void uploadBundle(final BundleVersionClient bundleVersionClient, final String bucketId, final BundleType bundleType,
                              final File directory, final String fileExtension, final boolean recurse, final boolean skipSha256,
                              final boolean verbose, final AtomicInteger counter) {

        for (final File file : directory.listFiles()) {
            if (file.isDirectory() && recurse) {
                uploadBundle(bundleVersionClient, bucketId, bundleType, file, fileExtension, recurse, skipSha256, verbose, counter);
            } else {
                // if a file extension was provided then skip anything that doesn't end in the extension
                if (!StringUtils.isBlank(fileExtension) && !file.getName().endsWith(fileExtension)) {
                    continue;
                }

                // calculate the sha256 unless we were told to skip it
                String sha256 = null;
                if (!skipSha256) {
                    sha256 = calculateSha256(file, verbose);
                    // if we weren't skipping sha256 and we got null, then an error happened so don't upload the bundle
                    if (sha256 == null) {
                        continue;
                    }
                }

                // upload the binary bundle
                try (final InputStream bundleInputStream = new FileInputStream(file)) {
                    final BundleVersion createdVersion = bundleVersionClient.create(bucketId, bundleType, bundleInputStream, sha256);
                    counter.incrementAndGet();

                    if (getContext().isInteractive()) {
                        println("Successfully uploaded "
                                + createdVersion.getBundle().getGroupId() + "::"
                                + createdVersion.getBundle().getArtifactId() + "::"
                                + createdVersion.getVersionMetadata().getVersion());
                    }

                } catch (Exception e) {
                    println("Error uploading bundle from " + file.getAbsolutePath());
                    if (verbose) {
                        e.printStackTrace(getContext().getOutput());
                    }
                }
            }
        }

    }

    private String calculateSha256(final File file, final boolean verbose) {
        try (final InputStream inputStream = new FileInputStream(file)) {
            return Hex.encodeHexString(DigestUtils.sha256(inputStream));
        } catch (Exception e) {
            println("Error calculating SHA-256 for " + file.getAbsolutePath());
            if (verbose) {
                e.printStackTrace(getContext().getOutput());
            }
            return null;
        }
    }

}
