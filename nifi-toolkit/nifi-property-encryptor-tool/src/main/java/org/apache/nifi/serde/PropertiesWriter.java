package org.apache.nifi.serde;

import org.apache.nifi.properties.ReadableProperties;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Set;

public class PropertiesWriter {

    private static final String PROPERTY_FORMAT = "%s=%s";

    public static void writePropertiesFile(final InputStream inputStream, final OutputStream outputStream, final ReadableProperties properties) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Set<String> keys = properties.getPropertyKeys();
                for (final String key : keys) {
                    if (line.startsWith(key)) {
                        line = String.format(PROPERTY_FORMAT, key, properties.getProperty(key));
                    }
                }
                writer.write(line);
                writer.newLine();
            }
        }
    }

}
