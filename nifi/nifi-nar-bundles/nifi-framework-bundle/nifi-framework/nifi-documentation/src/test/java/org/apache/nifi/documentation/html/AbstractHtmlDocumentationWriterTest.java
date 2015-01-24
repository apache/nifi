package org.apache.nifi.documentation.html;

import java.io.IOException;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.documentation.example.FullyDocumentedControllerService;
import org.apache.nifi.documentation.example.FullyDocumentedReportingTask;
import org.apache.nifi.documentation.mock.MockControllerServiceInitializationContext;
import org.apache.nifi.documentation.mock.MockReportingInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.Test;

public class AbstractHtmlDocumentationWriterTest {

	@Test
	public void testDocumentControllerService() throws InitializationException, IOException {

		ControllerService controllerService = new FullyDocumentedControllerService();
		controllerService.initialize(new MockControllerServiceInitializationContext());
		
		DocumentationWriter writer = new HtmlDocumentationWriter();

		writer.write(controllerService, System.out, false);

	}

	@Test
	public void testDocumentReportingTask() throws InitializationException, IOException {
		
		ReportingTask reportingTask = new FullyDocumentedReportingTask();
		reportingTask.initialize(new MockReportingInitializationContext());
		
		DocumentationWriter writer = new HtmlDocumentationWriter();

		writer.write(reportingTask, System.out, false);
	}

}
