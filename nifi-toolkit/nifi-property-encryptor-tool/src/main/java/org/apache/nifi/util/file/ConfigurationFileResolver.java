package org.apache.nifi.util.file;

import org.apache.nifi.properties.ApplicationProperties;
import java.io.File;
import java.util.List;

public interface ConfigurationFileResolver<E extends ApplicationProperties> {

    List<File> resolveConfigurationFilesFromApplicationProperties(final E properties) throws ConfigurationFileResolverException;

    default boolean isValidConfigurationFile(final File configurationFile) throws ConfigurationFileResolverException {
        return configurationFile.isFile() && configurationFile.canRead();
    }
}
