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
package org.apache.nifi.registry.client.impl;

import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.ContentDisposition;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.util.regex.Pattern;

public class ClientUtils {

    private static final char FORWARD_SLASH = '/';

    private static final char BACKWARD_SLASH = '\\';

    private static final Pattern FILENAME_PARAMETER_PATTERN = Pattern.compile(";\\s*filename\\s*=\\s*", Pattern.CASE_INSENSITIVE);

    public static File getExtensionBundleVersionContent(final Response response, final File outputDirectory) {
        final String contentDispositionHeader = response.getHeaderString("Content-Disposition");
        if (StringUtils.isBlank(contentDispositionHeader)) {
            throw new IllegalStateException("Content-Disposition header was blank or missing");
        }

        final File bundleFile = getContentDispositionFile(contentDispositionHeader, outputDirectory);

        try (final InputStream responseInputStream = response.readEntity(InputStream.class)) {
            Files.copy(responseInputStream, bundleFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return bundleFile;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to write bundle content due to: " + e.getMessage(), e);
        }
    }

    private static File getContentDispositionFile(final String contentDispositionHeader, final File outputDirectory) {
        if (contentDispositionHeader.indexOf(BACKWARD_SLASH) >= 0) {
            throw new IllegalStateException("Content-Disposition filename was invalid");
        }

        final String filename;
        try {
            final String normalizedHeader = FILENAME_PARAMETER_PATTERN.matcher(contentDispositionHeader).replaceFirst("; filename=");
            filename = new ContentDisposition(normalizedHeader).getFileName();
        } catch (final ParseException e) {
            throw new IllegalStateException("Content-Disposition header was invalid", e);
        }

        if (StringUtils.isBlank(filename) || filename.equals(".") || filename.equals("..")
                || filename.indexOf(FORWARD_SLASH) >= 0 || filename.indexOf(BACKWARD_SLASH) >= 0
                || filename.chars().anyMatch(character -> Character.isISOControl(character))) {
            throw new IllegalStateException("Content-Disposition filename was invalid");
        }

        final Path outputPath = outputDirectory.toPath().toAbsolutePath().normalize();
        final Path filenamePath;
        try {
            filenamePath = Path.of(filename);
        } catch (final RuntimeException e) {
            throw new IllegalStateException("Content-Disposition filename was invalid", e);
        }

        final Path bundlePath = outputPath.resolve(filenamePath).normalize();
        if (filenamePath.isAbsolute() || filenamePath.getNameCount() != 1 || !outputPath.equals(bundlePath.getParent())) {
            throw new IllegalStateException("Content-Disposition filename was invalid");
        }
        return bundlePath.toFile();
    }

}
