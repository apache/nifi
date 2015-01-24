package org.apache.nifi.documentation.html;

import java.io.IOException;

import org.apache.nifi.documentation.DocumentationWriter;
import org.apache.nifi.documentation.example.FullyDocumentedProcessor;
import org.apache.nifi.documentation.mock.MockProcessorInitializationContext;
import org.junit.Test;

public class ProcessorDocumentationWriterTest {

	@Test
	public void testFullyDocumentedProcessor() throws IOException {
		FullyDocumentedProcessor processor = new FullyDocumentedProcessor();
		processor.initialize(new MockProcessorInitializationContext());

		DocumentationWriter writer = new HtmlProcessorDocumentationWriter();

		writer.write(processor, System.out, false);
	}

}
