package org.apache.nifi.documentation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.html.HtmlDocumentationWriter;
import org.apache.nifi.documentation.html.HtmlProcessorDocumentationWriter;
import org.apache.nifi.documentation.init.ControllerServiceInitializer;
import org.apache.nifi.documentation.init.ProcessorInitializer;
import org.apache.nifi.documentation.init.ReportingTaskingInitializer;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocGenerator {

	private static final Logger logger = LoggerFactory.getLogger(DocGenerator.class);
	
	public static void generate(NiFiProperties properties) {
		@SuppressWarnings("rawtypes")
		Set<Class> extensionClasses = new HashSet<>();
		extensionClasses.addAll(ExtensionManager.getExtensions(Processor.class));
		extensionClasses.addAll(ExtensionManager.getExtensions(ControllerService.class));
		extensionClasses.addAll(ExtensionManager.getExtensions(ReportingTask.class));

		final File explodedNiFiDocsDir = properties.getComponentDocumentationWorkingDirectory();

		logger.info("Generating documentation for: " + extensionClasses.size() + " components in: " + explodedNiFiDocsDir);
		
		for (Class<?> extensionClass : extensionClasses) {
			if (ConfigurableComponent.class.isAssignableFrom(extensionClass)) {
				Class<? extends ConfigurableComponent> componentClass = extensionClass
						.asSubclass(ConfigurableComponent.class);
				try {
					logger.info("Documenting: " + componentClass);
					document(explodedNiFiDocsDir, componentClass);
					logger.info("Documented: " + componentClass);
				} catch (Exception e) {
					// TODO deal with exceptions
					logger.error("Unable to document: " + componentClass);
				}
			}
		}
	}

	private static void document(File docsDir, Class<? extends ConfigurableComponent> componentClass)
			throws InstantiationException, IllegalAccessException, IOException, InitializationException {

		ConfigurableComponent component = componentClass.newInstance();
		ConfigurableComponentInitializer initializer = getComponentInitializer(componentClass);
		initializer.initialize(component);

		DocumentationWriter writer = getDocumentWriter(componentClass);

		File directory = new File(docsDir, componentClass.getCanonicalName());
		directory.mkdirs();

		File baseDocumenationFile = new File(directory, "index.html");
		if (baseDocumenationFile.exists()) {
			logger.warn("WARNING: " + baseDocumenationFile + " already exists!");
		}

		OutputStream output = new FileOutputStream(baseDocumenationFile);

		// TODO figure out what to pull in here...
		writer.write(component, output, hasAdditionalInfo(directory));
		output.close();
	}

	
	private static DocumentationWriter getDocumentWriter(Class<? extends ConfigurableComponent> componentClass) {
		if (Processor.class.isAssignableFrom(componentClass)) {
			return new HtmlProcessorDocumentationWriter();
		} else if (ControllerService.class.isAssignableFrom(componentClass)) {
			return new HtmlDocumentationWriter();
		} else if (ReportingTask.class.isAssignableFrom(componentClass)) {
			return new HtmlDocumentationWriter();
		}

		return null;
	}

	private static ConfigurableComponentInitializer getComponentInitializer(
			Class<? extends ConfigurableComponent> componentClass) {
		if (Processor.class.isAssignableFrom(componentClass)) {
			return new ProcessorInitializer();
		} else if (ControllerService.class.isAssignableFrom(componentClass)) {
			return new ControllerServiceInitializer();
		} else if (ReportingTask.class.isAssignableFrom(componentClass)) {
			return new ReportingTaskingInitializer();
		}

		return null;
	}
	
	/**
	 * Checks to see if a directory to write to has additional information in it already.
	 * @param directory
	 * @return
	 */
	private static boolean hasAdditionalInfo(File directory) {
		return directory.list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.equalsIgnoreCase("additionalDetails.html");
			}

		}).length > 0;
	}
}
