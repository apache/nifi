package org.apache.nifi.properties;

import java.io.File;

public interface PropertiesLoader<T> {
    T load(final File file);

}
