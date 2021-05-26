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
package org.apache.nifi.registry.bundle.extract.nar;

import org.apache.nifi.registry.bundle.extract.BundleException;
import org.apache.nifi.registry.bundle.extract.BundleExtractor;
import org.apache.nifi.registry.bundle.extract.nar.docs.ExtensionManifestParser;
import org.apache.nifi.registry.bundle.extract.nar.docs.JacksonExtensionManifestParser;
import org.apache.nifi.registry.bundle.model.BundleIdentifier;
import org.apache.nifi.registry.bundle.model.BundleDetails;
import org.apache.nifi.registry.extension.bundle.BuildInfo;
import org.apache.nifi.registry.extension.component.manifest.ExtensionManifest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of ExtensionBundleExtractor for NAR bundles.
 */
public class NarBundleExtractor implements BundleExtractor {

    /**
     * The name of the JarEntry that contains the extension-docs.xml file.
     */
    private static String EXTENSION_DESCRIPTOR_ENTRY = "META-INF/docs/extension-manifest.xml";

    /**
     * The pattern of a JarEntry for additionalDetails.html entries.
     */
    private static Pattern ADDITIONAL_DETAILS_ENTRY_PATTERN =
            Pattern.compile("META-INF\\/docs\\/additional-details\\/(.+)\\/additionalDetails.html");

    /**
     * The format of the date string in the NAR MANIFEST for Built-Timestamp.
     */
    private static String BUILT_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * Used in place of any build info that is not present.
     */
    static String NA = "N/A";


    @Override
    public BundleDetails extract(final InputStream inputStream) throws IOException {
        try (final JarInputStream jarInputStream = new JarInputStream(inputStream)) {
            final Manifest manifest = jarInputStream.getManifest();
            if (manifest == null) {
                throw new BundleException("NAR bundles must contain a valid MANIFEST");
            }

            final Attributes attributes = manifest.getMainAttributes();
            final BundleIdentifier bundleIdentifier = getBundleCoordinate(attributes);
            final BundleIdentifier dependencyCoordinate = getDependencyBundleCoordinate(attributes);
            final BuildInfo buildInfo = getBuildInfo(attributes);

            final BundleDetails.Builder builder = new BundleDetails.Builder()
                    .coordinate(bundleIdentifier)
                    .addDependencyCoordinate(dependencyCoordinate)
                    .buildInfo(buildInfo);

            parseExtensionDocs(jarInputStream, builder);

            return builder.build();
        }
    }

    private BundleIdentifier getBundleCoordinate(final Attributes attributes) {
        try {
            final String groupId = attributes.getValue(NarManifestEntry.NAR_GROUP.getManifestName());
            final String artifactId = attributes.getValue(NarManifestEntry.NAR_ID.getManifestName());
            final String version = attributes.getValue(NarManifestEntry.NAR_VERSION.getManifestName());

            return new BundleIdentifier(groupId, artifactId, version);
        } catch (Exception e) {
            throw new BundleException("Unable to obtain bundle coordinate due to: " + e.getMessage(), e);
        }
    }

    private BundleIdentifier getDependencyBundleCoordinate(final Attributes attributes) {
        try {
            final String dependencyGroupId = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_GROUP.getManifestName());
            final String dependencyArtifactId = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_ID.getManifestName());
            final String dependencyVersion = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_VERSION.getManifestName());

            final BundleIdentifier dependencyCoordinate;
            if (dependencyArtifactId != null) {
                dependencyCoordinate = new BundleIdentifier(dependencyGroupId, dependencyArtifactId, dependencyVersion);
            } else {
                dependencyCoordinate = null;
            }
            return dependencyCoordinate;
        } catch (Exception e) {
            throw new BundleException("Unable to obtain bundle coordinate for dependency due to: " + e.getMessage(), e);
        }
    }

    private BuildInfo getBuildInfo(final Attributes attributes) {
        final String buildBranch = attributes.getValue(NarManifestEntry.BUILD_BRANCH.getManifestName());
        final String buildTag = attributes.getValue(NarManifestEntry.BUILD_TAG.getManifestName());
        final String buildRevision = attributes.getValue(NarManifestEntry.BUILD_REVISION.getManifestName());
        final String buildTimestamp = attributes.getValue(NarManifestEntry.BUILD_TIMESTAMP.getManifestName());
        final String buildJdk = attributes.getValue(NarManifestEntry.BUILD_JDK.getManifestName());
        final String builtBy = attributes.getValue(NarManifestEntry.BUILT_BY.getManifestName());

        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(BUILT_TIMESTAMP_FORMAT);
        try {
            final Date buildDate = simpleDateFormat.parse(buildTimestamp);

            final BuildInfo buildInfo = new BuildInfo();
            buildInfo.setBuildTool(isBlank(buildJdk) ? NA : buildJdk);
            buildInfo.setBuildBranch(isBlank(buildBranch) ? NA : buildBranch);
            buildInfo.setBuildTag(isBlank(buildTag) ? NA : buildTag);
            buildInfo.setBuildRevision(isBlank(buildRevision) ? NA : buildRevision);
            buildInfo.setBuilt(buildDate.getTime());
            buildInfo.setBuiltBy(isBlank(builtBy) ? NA : builtBy);
            buildInfo.setBuildFlags(NA);
            return buildInfo;

        } catch (ParseException e) {
            throw new BundleException("Unable to parse " + NarManifestEntry.BUILD_TIMESTAMP.getManifestName(), e);
        } catch (Exception e) {
            throw new BundleException("Unable to create build info for bundle due to: " + e.getMessage(), e);
        }
    }

    public boolean isBlank(String value) {
        return (value == null || value.trim().isEmpty());
    }

    private void parseExtensionDocs(final JarInputStream jarInputStream, final BundleDetails.Builder builder) throws IOException {
        JarEntry jarEntry;
        boolean foundExtensionDocs = false;
        while((jarEntry = jarInputStream.getNextJarEntry()) != null) {
            final String jarEntryName = jarEntry.getName();
            if (EXTENSION_DESCRIPTOR_ENTRY.equals(jarEntryName)) {
                try {
                    final byte[] rawDocsContent = toByteArray(jarInputStream);
                    final ExtensionManifestParser docsParser = new JacksonExtensionManifestParser();
                    final InputStream inputStream = new NonCloseableInputStream(new ByteArrayInputStream(rawDocsContent));

                    final ExtensionManifest extensionManifest = docsParser.parse(inputStream);
                    builder.addExtensions(extensionManifest.getExtensions());
                    builder.systemApiVersion(extensionManifest.getSystemApiVersion());

                    foundExtensionDocs = true;
                } catch (Exception e) {
                    throw new BundleException("Unable to obtain extension info for bundle due to: " + e.getMessage(), e);
                }
            } else {
                final Matcher matcher = ADDITIONAL_DETAILS_ENTRY_PATTERN.matcher(jarEntryName);
                if (matcher.matches()) {
                    final String extensionName = matcher.group(1);
                    final String additionalDetailsContent = new String(toByteArray(jarInputStream), StandardCharsets.UTF_8);
                    builder.addAdditionalDetails(extensionName, additionalDetailsContent);
                }
            }
        }

        if (!foundExtensionDocs) {
            throw new BundleException("Unable to find descriptor at '" + EXTENSION_DESCRIPTOR_ENTRY + "'. " +
                    "This NAR may need to be rebuilt with the latest version of the NiFi NAR Maven Plugin.");
        }
    }

    private byte[] toByteArray(final InputStream input) throws IOException {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[16384];
        while ((nRead = input.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        return buffer.toByteArray();
    }

    private static class NonCloseableInputStream extends FilterInputStream {

        private final InputStream toWrap;

        public NonCloseableInputStream(final InputStream toWrap) {
            super(toWrap);
            this.toWrap = toWrap;
        }

        @Override
        public int read() throws IOException {
            return toWrap.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return toWrap.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return toWrap.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }
}
