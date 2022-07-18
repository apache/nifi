package org.apache.nifi.services.smb;

import java.io.OutputStream;
import java.util.stream.Stream;

public interface NiFiSmbClient {

    Stream<SmbListableEntity> listRemoteFiles(String path);

    void createDirectory(String path);

    OutputStream getOutputStreamForFile(String pathAndFileName);

    void close();
}
