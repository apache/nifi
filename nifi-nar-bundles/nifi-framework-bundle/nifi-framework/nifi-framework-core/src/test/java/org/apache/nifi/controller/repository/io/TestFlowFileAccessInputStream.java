package org.apache.nifi.controller.repository.io;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestFlowFileAccessInputStream {

    private InputStream in;
    private FlowFile flowFile;
    private ContentClaim claim;
    private final byte[] data = new byte[16];

    @Before
    public void setup() throws IOException {
        in = mock(InputStream.class);
        flowFile = mock(FlowFile.class);
        claim = mock(ContentClaim.class);

        Mockito.when(in.read(data, 0, 16)).thenAnswer(new Answer<Integer>() {
            private int count = 0;

            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                if (count == 0) {
                    count++;
                    return 16;
                }
                return -1;
            }
        });
        Mockito.when(flowFile.getSize()).thenReturn(32l);
    }

    @Test
    public void testThrowExceptionWhenLessContentReadFromFlowFile() throws Exception {
        FlowFileAccessInputStream flowFileAccessInputStream = new FlowFileAccessInputStream(in, flowFile, claim);
        try {
            while (flowFileAccessInputStream.read(data) != -1) {
            }
            fail("Should throw ContentNotFoundException when lesser bytes read from flow file.");
        } catch (ContentNotFoundException e) {
            assertTrue(e.getMessage().contains("Stream contained only 16 bytes but should have contained 32"));
        } finally {
            flowFileAccessInputStream.close();
        }
    }
}
