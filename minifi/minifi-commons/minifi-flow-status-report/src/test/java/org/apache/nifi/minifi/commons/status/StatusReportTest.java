/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.minifi.commons.status;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addConnectionStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addControllerServiceStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addExpectedRemoteProcessGroupStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addInstanceStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addProcessorStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addReportingTaskStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addSystemDiagnosticStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StatusReportTest {

    @Test
    public void verifySerializableFullyPopulated() throws IOException, ClassNotFoundException {
        final FlowStatusReport original = new FlowStatusReport();

        addControllerServiceStatus(original, true, true, true, true);
        addInstanceStatus(original, true, true, true, true);
        addSystemDiagnosticStatus(original, true, true, true, true, true);
        addReportingTaskStatus(original, true, true, true, true);
        addConnectionStatus(original, true, true);
        addProcessorStatus(original, true, true, true, true, true);
        addExpectedRemoteProcessGroupStatus(original, true, true, true, true, true, true);

        final byte[] byteArrayCopy = serialize(original);
        final FlowStatusReport copy = unSerialize(byteArrayCopy, FlowStatusReport.class);

        assertEquals(original, copy);
    }

    @Test
    public void verifySerializableSomeNull() throws IOException, ClassNotFoundException {
        final FlowStatusReport original = new FlowStatusReport();

        addControllerServiceStatus(original, true, true, true, true);
        addInstanceStatus(original, true, true, true, true);
        addSystemDiagnosticStatus(original, true, true, true, true, true);
        addProcessorStatus(original, true, true, true, true, true);
        addExpectedRemoteProcessGroupStatus(original, true, true, true, true, true, true);

        final byte[] byteArrayCopy = serialize(original);
        final FlowStatusReport copy = unSerialize(byteArrayCopy, FlowStatusReport.class);

        assertEquals(original, copy);
    }

    private static <T extends Serializable> byte[] serialize(final T obj) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return baos.toByteArray();
    }

    private static <T extends Serializable> T unSerialize(final byte[] b, final Class<T> cl) throws IOException, ClassNotFoundException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);
        final Object o = ois.readObject();
        return cl.cast(o);
    }
}
