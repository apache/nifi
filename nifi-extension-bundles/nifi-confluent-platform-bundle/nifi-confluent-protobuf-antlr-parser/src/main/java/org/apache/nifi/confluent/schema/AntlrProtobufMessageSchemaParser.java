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
package org.apache.nifi.confluent.schema;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.nifi.confluent.schema.antlr.Protobuf3BaseVisitor;
import org.apache.nifi.confluent.schema.antlr.Protobuf3Lexer;
import org.apache.nifi.confluent.schema.antlr.Protobuf3Parser;
import org.apache.nifi.confluent.schema.antlr.Protobuf3Parser.MessageDefContext;
import org.apache.nifi.confluent.schema.antlr.Protobuf3Parser.PackageStatementContext;
import org.apache.nifi.confluent.schema.antlr.Protobuf3Parser.ProtoContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

/**
 * Implementation of ProtobufMessageSchemaParser that uses ANTLR to parse protobuf schemas
 * and extract message definitions including nested messages.
 */
public class AntlrProtobufMessageSchemaParser implements ProtobufMessageSchemaParser {

    @Override
    public List<ProtobufMessageSchema> parse(final String schemaText) {

        final CharStream input = CharStreams.fromString(schemaText);
        final Protobuf3Lexer lexer = new Protobuf3Lexer(input);
        final Protobuf3Parser parser = getProtobuf3Parser(lexer);

        final ProtoContext tree = parser.proto();

        // Create visitor and analyze
        final SchemaVisitor visitor = new SchemaVisitor();
        visitor.visit(tree);

        return visitor.getProtoMessages();

    }

    private Protobuf3Parser getProtobuf3Parser(final Protobuf3Lexer lexer) {
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final Protobuf3Parser parser = new Protobuf3Parser(tokens);

        // Add error listener to capture parsing errors
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line, final int charPositionInLine, final String msg, final RecognitionException e) {
                throw new RuntimeException(String.format("Syntax error at line %d, position %d: %s", line, charPositionInLine, msg), e);
            }
        });
        return parser;
    }

    private static class SchemaVisitor extends Protobuf3BaseVisitor<Void> {

        private final List<ProtobufMessageSchema> rootMessages = new ArrayList<>();
        private final Stack<ProtobufMessageSchema> messageStack = new Stack<>();
        private String currentPackage;

        @Override
        public Void visitPackageStatement(final PackageStatementContext ctx) {
            if (ctx.fullIdent() != null) {
                currentPackage = ctx.fullIdent().getText();
            }
            return null;
        }

        @Override
        public Void visitMessageDef(final MessageDefContext ctx) {
            final String messageName = ctx.messageName().getText();

            final Optional<String> packageName = Optional.ofNullable(currentPackage);
            final ProtobufMessageSchema protobufMessageSchema = new ProtobufMessageSchema(messageName, packageName);

            // Add to parent's nested messages or root messages
            if (messageStack.isEmpty()) {
                rootMessages.add(protobufMessageSchema);
            } else {
                final ProtobufMessageSchema parent = messageStack.peek();
                parent.addChildMessage(protobufMessageSchema);
            }

            messageStack.push(protobufMessageSchema);
            // Visit nested messages
            super.visitMessageDef(ctx);
            messageStack.pop();

            return null;
        }

        public List<ProtobufMessageSchema> getProtoMessages() {
            return rootMessages.stream().toList();
        }
    }
}
