package org.apache.nifi.serde;

import org.apache.nifi.properties.ReadableProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface PropertiesWriter {
    void writePropertiesFile(final InputStream inputStream, final OutputStream outputStream, final ReadableProperties properties) throws IOException;
}
