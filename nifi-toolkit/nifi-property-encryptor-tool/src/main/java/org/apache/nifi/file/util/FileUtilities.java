package org.apache.nifi.file.util;

import java.io.File;
import java.io.IOException;

public class FileUtilities {

    public static File getTemporaryOutputFile(final String prefix, final File partnerFile) throws IOException {
        assert(partnerFile != null);
        assert(partnerFile.isFile());
        return File.createTempFile(prefix, partnerFile.getName(), partnerFile.getParentFile());

//        final String originalName = partnerFile.getName();
//        final String tempFileName = String.format(".tmp-%s", originalName);
//        File outputFile = partnerFile.getParentFile();
//        return File.createTempFile()

    }

    static boolean isSafeToWrite(final File fileToWrite) {
        assert(fileToWrite != null);
        return (!fileToWrite.exists() && fileToWrite.getParentFile().canWrite() || (fileToWrite.exists() && fileToWrite.canWrite()));
    }
}
