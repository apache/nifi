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
package org.apache.nifi.toolkit.cli.impl.result.writer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class TestDynamicTableWriter {

    private Table table;
    private TableWriter tableWriter;

    private ByteArrayOutputStream outputStream;
    private PrintStream printStream;

    @Before
    public void setup() {
        this.table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .column("Description", 11, 40, true)
                .build();

        this.tableWriter = new DynamicTableWriter();

        this.outputStream = new ByteArrayOutputStream();
        this.printStream = new PrintStream(outputStream, true);
    }

    @Test
    public void testWriteEmptyTable() {
        tableWriter.write(table, printStream);

        final String result = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        //System.out.println(result);

        final String expected = "\n" +
                "#     Name                   Id                                     Description   \n" +
                "---   --------------------   ------------------------------------   -----------   \n" +
                "\n";

        Assert.assertEquals(expected, result);
    }

    @Test
    public void testDynamicTableWriter() {
        table.addRow(
                "1",
                "Bucket 1",
                "12345-12345-12345-12345-12345-12345",
                ""
        );

        table.addRow(
                "2",
                "Bucket 2 - This is a really really really long name",
                "12345-12345-12345-12345-12345-12345",
                "This is a really really really really really really really really really really long description"
        );

        table.addRow(
                "3",
                "Bucket 3",
                "12345-12345-12345-12345-12345-12345",
                null
        );

        tableWriter.write(table, printStream);

        final String result = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        //System.out.println(result);

        final String expected = "\n" +
                "#   Name                                   Id                                    Description                                \n" +
                "-   ------------------------------------   -----------------------------------   ----------------------------------------   \n" +
                "1   Bucket 1                               12345-12345-12345-12345-12345-12345                                              \n" +
                "2   Bucket 2 - This is a really reall...   12345-12345-12345-12345-12345-12345   This is a really really really really...   \n" +
                "3   Bucket 3                               12345-12345-12345-12345-12345-12345   (empty)                                    \n" +
                "\n";

        Assert.assertEquals(expected, result);
    }

    @Test
    public void testWhenAllDescriptionsAreEmpty() {
        table.addRow("1", "Bucket 1", "12345-12345-12345-12345-12345-12345", null);
        table.addRow("2", "Bucket 2", "12345-12345-12345-12345-12345-12345", null);
        tableWriter.write(table, printStream);

        final String result = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        //System.out.println(result);

        final String expected ="\n" +
                "#   Name       Id                                    Description   \n" +
                "-   --------   -----------------------------------   -----------   \n" +
                "1   Bucket 1   12345-12345-12345-12345-12345-12345   (empty)       \n" +
                "2   Bucket 2   12345-12345-12345-12345-12345-12345   (empty)       \n" +
                "\n";

        Assert.assertEquals(expected, result);
    }

}