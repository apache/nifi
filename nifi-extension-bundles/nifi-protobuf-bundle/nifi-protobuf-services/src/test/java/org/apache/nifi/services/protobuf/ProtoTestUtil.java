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
package org.apache.nifi.services.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.squareup.wire.schema.CoreLoaderKt;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.nifi.services.protobuf.converter.ProtobufDataConverter.MAP_KEY_FIELD_NAME;
import static org.apache.nifi.services.protobuf.converter.ProtobufDataConverter.MAP_VALUE_FIELD_NAME;

public class ProtoTestUtil {

    public static final String BASE_TEST_PATH = "src/test/resources/";

    public static Schema loadProto3TestSchema() {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Collections.singletonList(Location.get(BASE_TEST_PATH + "test_proto3.proto")), Collections.emptyList());
        return schemaLoader.loadSchema();
    }

    public static Schema loadRepeatedProto3TestSchema() {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Collections.singletonList(Location.get(BASE_TEST_PATH + "test_repeated_proto3.proto")), Collections.emptyList());
        return schemaLoader.loadSchema();
    }

    public static Schema loadProto2TestSchema() {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Arrays.asList(
                Location.get(BASE_TEST_PATH, "test_proto2.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/any.proto")), Collections.emptyList());
        return schemaLoader.loadSchema();
    }

    public static Schema loadCircularReferenceTestSchema() {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Collections.singletonList(Location.get(BASE_TEST_PATH + "test_circular_reference.proto")), Collections.emptyList());
        return schemaLoader.loadSchema();
    }

    public static InputStream generateInputDataForProto3() throws IOException, Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(new FileInputStream(BASE_TEST_PATH + "test_proto3.desc"));
        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(descriptorSet.getFile(0), new Descriptors.FileDescriptor[0]);

        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("Proto3Message");
        Descriptors.Descriptor nestedMessageDescriptor = fileDescriptor.findMessageTypeByName("NestedMessage");
        Descriptors.EnumDescriptor enumValueDescriptor = fileDescriptor.findEnumTypeByName("TestEnum");
        Descriptors.Descriptor mapDescriptor = nestedMessageDescriptor.findNestedTypeByName("TestMapEntry");

        DynamicMessage mapEntry1 = DynamicMessage
                .newBuilder(mapDescriptor)
                .setField(mapDescriptor.findFieldByName(MAP_KEY_FIELD_NAME), "test_key_entry1")
                .setField(mapDescriptor.findFieldByName(MAP_VALUE_FIELD_NAME), 101)
                .build();

        DynamicMessage mapEntry2 = DynamicMessage
                .newBuilder(mapDescriptor)
                .setField(mapDescriptor.findFieldByName(MAP_KEY_FIELD_NAME), "test_key_entry2")
                .setField(mapDescriptor.findFieldByName(MAP_VALUE_FIELD_NAME), 202)
                .build();

        DynamicMessage nestedMessage = DynamicMessage
                .newBuilder(nestedMessageDescriptor)
                .setField(nestedMessageDescriptor.findFieldByNumber(20), enumValueDescriptor.findValueByNumber(2))
                .setField(nestedMessageDescriptor.findFieldByNumber(21), Arrays.asList(mapEntry1, mapEntry2))
                .setField(nestedMessageDescriptor.findFieldByNumber(22), "One Of Option")
                .setField(nestedMessageDescriptor.findFieldByNumber(23), true)
                .setField(nestedMessageDescriptor.findFieldByNumber(24), 3)
                .build();

        DynamicMessage message = DynamicMessage
                .newBuilder(messageDescriptor)
                .setField(messageDescriptor.findFieldByNumber(1), true)
                .setField(messageDescriptor.findFieldByNumber(2), "Test text")
                .setField(messageDescriptor.findFieldByNumber(3), Integer.MAX_VALUE)
                .setField(messageDescriptor.findFieldByNumber(4), -1)
                .setField(messageDescriptor.findFieldByNumber(5), Integer.MIN_VALUE)
                .setField(messageDescriptor.findFieldByNumber(6), -2)
                .setField(messageDescriptor.findFieldByNumber(7), Integer.MAX_VALUE)
                .setField(messageDescriptor.findFieldByNumber(8), Double.MAX_VALUE)
                .setField(messageDescriptor.findFieldByNumber(9), Float.MAX_VALUE)
                .setField(messageDescriptor.findFieldByNumber(10), "Test bytes".getBytes())
                .setField(messageDescriptor.findFieldByNumber(11), Long.MAX_VALUE)
                .setField(messageDescriptor.findFieldByNumber(12), -1L)
                .setField(messageDescriptor.findFieldByNumber(13), Long.MIN_VALUE)
                .setField(messageDescriptor.findFieldByNumber(14), -2L)
                .setField(messageDescriptor.findFieldByNumber(15), Long.MAX_VALUE)
                .setField(messageDescriptor.findFieldByNumber(16), nestedMessage)
                .build();

        return message.toByteString().newInput();
    }

    public static InputStream generateInputDataForRepeatedProto3() throws IOException, Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(new FileInputStream(BASE_TEST_PATH + "test_repeated_proto3.desc"));
        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(descriptorSet.getFile(0), new Descriptors.FileDescriptor[0]);

        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("RootMessage");
        Descriptors.Descriptor repeatedMessageDescriptor = fileDescriptor.findMessageTypeByName("RepeatedMessage");
        Descriptors.EnumDescriptor enumValueDescriptor = fileDescriptor.findEnumTypeByName("TestEnum");

        DynamicMessage repeatedMessage1 = DynamicMessage
                .newBuilder(repeatedMessageDescriptor)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(1), true)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(1), false)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(2), "Test text1")
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(2), "Test text2")
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(3), Integer.MAX_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(3), Integer.MAX_VALUE - 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(4), -1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(4), -2)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(5), Integer.MIN_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(5), Integer.MIN_VALUE + 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(6), -2)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(6), -3)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(7), Integer.MAX_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(7), Integer.MAX_VALUE - 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(8), Double.MAX_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(8), Double.MAX_VALUE - 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(9), Float.MAX_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(9), Float.MAX_VALUE - 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(10), "Test bytes1".getBytes())
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(10), "Test bytes2".getBytes())
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(11), Long.MAX_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(11), Long.MAX_VALUE - 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(12), -1L)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(12), -2L)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(13), Long.MIN_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(13), Long.MIN_VALUE + 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(14), -2L)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(14), -1L)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(15), Long.MAX_VALUE)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(15), Long.MAX_VALUE - 1)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(16), enumValueDescriptor.findValueByNumber(1))
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(16), enumValueDescriptor.findValueByNumber(2))
                .build();

        DynamicMessage repeatedMessage2 = DynamicMessage
                .newBuilder(repeatedMessageDescriptor)
                .addRepeatedField(repeatedMessageDescriptor.findFieldByNumber(1), true)
                .build();

        DynamicMessage rootMessage = DynamicMessage
                .newBuilder(messageDescriptor)
                .addRepeatedField(messageDescriptor.findFieldByNumber(1), repeatedMessage1)
                .addRepeatedField(messageDescriptor.findFieldByNumber(1), repeatedMessage2)
                .build();

        return rootMessage.toByteString().newInput();
    }

    public static InputStream generateInputDataForProto2() throws IOException, Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorSet anyDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(new FileInputStream(BASE_TEST_PATH + "google/protobuf/any.desc"));
        Descriptors.FileDescriptor anyDesc = Descriptors.FileDescriptor.buildFrom(anyDescriptorSet.getFile(0), new Descriptors.FileDescriptor[]{});

        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(new FileInputStream(BASE_TEST_PATH + "test_proto2.desc"));
        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(descriptorSet.getFile(0), new Descriptors.FileDescriptor[]{anyDesc});

        Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("Proto2Message");
        Descriptors.Descriptor anyTestDescriptor = fileDescriptor.findMessageTypeByName("AnyValueMessage");
        Descriptors.FieldDescriptor fieldDescriptor = fileDescriptor.findExtensionByName("extensionField");
        Descriptors.Descriptor anyDescriptor = anyDesc.findMessageTypeByName("Any");

        DynamicMessage anyTestMessage = DynamicMessage
                .newBuilder(anyTestDescriptor)
                .setField(anyTestDescriptor.findFieldByNumber(1), "Test field 1")
                .setField(anyTestDescriptor.findFieldByNumber(2), "Test field 2")
                .build();

        DynamicMessage anyMessage = DynamicMessage
                .newBuilder(anyDescriptor)
                .setField(anyDescriptor.findFieldByNumber(1), "type.googleapis.com/AnyValueMessage")
                .setField(anyDescriptor.findFieldByNumber(2), anyTestMessage.toByteArray())
                .build();

        DynamicMessage message = DynamicMessage
                .newBuilder(messageDescriptor)
                .setField(messageDescriptor.findFieldByNumber(1), true)
                .setField(messageDescriptor.findFieldByNumber(3), anyMessage)
                .setField(fieldDescriptor, Integer.MAX_VALUE)
                .build();

        return message.toByteString().newInput();
    }
}
