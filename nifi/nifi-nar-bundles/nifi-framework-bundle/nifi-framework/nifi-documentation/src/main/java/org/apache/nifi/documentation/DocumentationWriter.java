package org.apache.nifi.documentation;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.components.ConfigurableComponent;

public interface DocumentationWriter {

	void write(ConfigurableComponent configurableComponent, OutputStream streamToWriteTo,
			boolean includesAdditionalDocumentation) throws IOException;
}
