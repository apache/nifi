package org.apache.nifi.file.util;

import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.util.NiFiBootstrapUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

public class FileUtilities {

    public static File getTemporaryOutputFile(final String prefix, final File siblingFile) throws IOException {
        if (siblingFile != null && siblingFile.isFile()) {
            return File.createTempFile(prefix, siblingFile.getName(), siblingFile.getParentFile());
        } else {
            throw new IOException("Failed to create temporary output file because sibling file is null or is not a file");
        }
    }

    public static boolean isSafeToWrite(final File fileToWrite) {
        assert(fileToWrite != null);
        return (!fileToWrite.exists() && fileToWrite.getParentFile().canWrite() || (fileToWrite.exists() && fileToWrite.canWrite()));
    }

    public static boolean directoryContainsFilename(final File directory, final String filename) {
        return Arrays.asList(directory.list()).contains(filename);
    }

    public static boolean isNiFiRegistryDirectory(final File baseDirectory) {
        return directoryContainsFilename(baseDirectory, "nifi-registry.properties");
    }

    public static boolean isNiFiDirectory(final File baseDirectory) {
        return directoryContainsFilename(baseDirectory, "nifi.properties");
    }
}
