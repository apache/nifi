package org.apache.nifi.properties;

import java.nio.file.Path;
import java.util.Properties;

/**
 * Properties representing bootstrap.conf, the properties which can be updated if required
 */
public class MutableBootstrapProperties extends BootstrapProperties {

    public MutableBootstrapProperties(String propertyPrefix, Properties properties, Path configFilePath) {
        super(propertyPrefix, properties, configFilePath);
    }

    public void setProperty(final String key, final String value) {
        rawProperties.setProperty(getPropertyKey(key), value);
    }
}
