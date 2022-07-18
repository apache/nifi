package org.apache.nifi.services.smb;

import java.io.OutputStream;
import java.util.stream.Stream;

/**
 *  Service abstraction for Server Message Block protocol operations.
 */
public interface SmbClientService {

    Stream<SmbListableEntity> listRemoteFiles(String path);

    void createDirectory(String path);

    OutputStream getOutputStreamForFile(String path);

    void close();
}
