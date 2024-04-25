# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from langchain.text_splitter import Language
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, PropertyDependency, ExpressionLanguageScope
from nifiapi.documentation import use_case, multi_processor_use_case, ProcessorConfiguration


SPLIT_BY_CHARACTER = 'Split by Character'
SPLIT_CODE = 'Split Code'
RECURSIVELY_SPLIT_BY_CHARACTER = 'Recursively Split by Character'

TEXT_KEY = "text"
METADATA_KEY = "metadata"

@use_case(
    description="Create chunks of text from a single larger chunk.",
    notes="The input for this use case is expected to be a FlowFile whose content is a JSON Lines document, with each line having a 'text' and a 'metadata' element.",
    keywords=["embedding", "vector", "text", "rag", "retrieval augmented generation"],
    configuration="""
        Set "Input Format" to "Plain Text"
        Set "Element Strategy" to "Single Document"
        """
)
@multi_processor_use_case(
    description="""
        Chunk Plaintext data in order to prepare it for storage in a vector store. The output is in "json-lines" format,
        containing the chunked data as text, as well as metadata pertaining to the chunk.""",
    notes="The input for this use case is expected to be a FlowFile whose content is a plaintext document.",
    keywords=["embedding", "vector", "text", "rag", "retrieval augmented generation"],
    configurations=[
        ProcessorConfiguration(
            processor_type="ParseDocument",
            configuration="""
                  Set "Input Format" to "Plain Text"
                  Set "Element Strategy" to "Single Document"

                  Connect the 'success' Relationship to ChunkDocument.
                  """
        ),
        ProcessorConfiguration(
            processor_type="ChunkDocument",
            configuration="""
                  Set the following properties:
                    "Chunking Strategy" = "Recursively Split by Character"
                    "Separator" = "\\n\\n,\\n, ,"
                    "Separator Format" = "Plain Text"
                    "Chunk Size" = "4000"
                    "Chunk Overlap" = "200"
                    "Keep Separator" = "false"

                  Connect the 'success' Relationship to the appropriate destination to store data in the desired vector store.
                  """
        )
    ])
@multi_processor_use_case(
    description="""
        Parse and chunk the textual contents of a PDF document in order to prepare it for storage in a vector store. The output is in "json-lines" format,
        containing the chunked data as text, as well as metadata pertaining to the chunk.""",
    notes="The input for this use case is expected to be a FlowFile whose content is a PDF document.",
    keywords=["pdf", "embedding", "vector", "text", "rag", "retrieval augmented generation"],
    configurations=[
        ProcessorConfiguration(
            processor_type="ParseDocument",
            configuration="""
                  Set "Input Format" to "PDF"
                  Set "Element Strategy" to "Single Document"
                  Set "Include Extracted Metadata" to "false"

                  Connect the 'success' Relationship to ChunkDocument.
                  """
        ),
        ProcessorConfiguration(
            processor_type="ChunkDocument",
            configuration="""
                  Set the following properties:
                    "Chunking Strategy" = "Recursively Split by Character"
                    "Separator" = "\\n\\n,\\n, ,"
                    "Separator Format" = "Plain Text"
                    "Chunk Size" = "4000"
                    "Chunk Overlap" = "200"
                    "Keep Separator" = "false"

                  Connect the 'success' Relationship to the appropriate destination to store data in the desired vector store.
                  """
        )
    ])
class ChunkDocument(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '@project.version@'
        description = """Chunks incoming documents that are formatted as JSON Lines into chunks that are appropriately sized for creating Text Embeddings.
            The input is expected to be in "json-lines" format, with each line having a 'text' and a 'metadata' element.
            Each line will then be split into one or more lines in the output."""
        tags = ["text", "split", "chunk", "langchain", "embeddings", "vector", "machine learning", "ML", "artificial intelligence", "ai", "document"]
        dependencies = ['langchain']


    CHUNK_STRATEGY = PropertyDescriptor(
        name="Chunking Strategy",
        description="Specifies which splitter should be used to split the text",
        allowable_values=[RECURSIVELY_SPLIT_BY_CHARACTER, SPLIT_BY_CHARACTER, SPLIT_CODE],
        required=True,
        default_value=RECURSIVELY_SPLIT_BY_CHARACTER
    )
    SEPARATOR = PropertyDescriptor(
        name="Separator",
        description="Specifies the character sequence to use for splitting apart the text. If using a Chunking Strategy of Recursively Split by Character, " +
                    "it is a comma-separated list of character sequences. Meta-characters \\n, \\r and \\t are automatically un-escaped.",
        required=True,
        default_value="\\n\\n,\\n, ,",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        dependencies=[PropertyDependency(CHUNK_STRATEGY, SPLIT_BY_CHARACTER, RECURSIVELY_SPLIT_BY_CHARACTER)],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    SEPARATOR_FORMAT = PropertyDescriptor(
        name="Separator Format",
        description="Specifies how to interpret the value of the <Separator> property",
        required=True,
        default_value="Plain Text",
        allowable_values=["Plain Text", "Regular Expression"],
        dependencies=[PropertyDependency(CHUNK_STRATEGY, SPLIT_BY_CHARACTER, RECURSIVELY_SPLIT_BY_CHARACTER)]
    )
    CHUNK_SIZE = PropertyDescriptor(
        name="Chunk Size",
        description="The maximum size of a chunk that should be returned",
        required=True,
        default_value="4000",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR]
    )
    CHUNK_OVERLAP = PropertyDescriptor(
        name="Chunk Overlap",
        description="The number of characters that should be overlapped between each chunk of text",
        required=True,
        default_value="200",
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR]
    )
    KEEP_SEPARATOR = PropertyDescriptor(
        name="Keep Separator",
        description="Whether or not to keep the text separator in each chunk of data",
        required=True,
        default_value="false",
        allowable_values=["true", "false"],
        dependencies=[PropertyDependency(CHUNK_STRATEGY, SPLIT_BY_CHARACTER, RECURSIVELY_SPLIT_BY_CHARACTER)]
    )
    STRIP_WHITESPACE = PropertyDescriptor(
        name="Strip Whitespace",
        description="Whether or not to strip the whitespace at the beginning and end of each chunk",
        required=True,
        default_value="true",
        allowable_values=["true", "false"],
        dependencies=[PropertyDependency(CHUNK_STRATEGY, SPLIT_BY_CHARACTER, RECURSIVELY_SPLIT_BY_CHARACTER)]
    )
    LANGUAGE = PropertyDescriptor(
        name="Language",
        description="The language to use for the Code's syntax",
        required=True,
        default_value="python",
        allowable_values=[e.value for e in Language],
        dependencies=[PropertyDependency(CHUNK_STRATEGY, SPLIT_CODE)]
    )

    property_descriptors = [CHUNK_STRATEGY,
                            SEPARATOR,
                            SEPARATOR_FORMAT,
                            CHUNK_SIZE,
                            CHUNK_OVERLAP,
                            KEEP_SEPARATOR,
                            STRIP_WHITESPACE,
                            LANGUAGE]


    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors


    def split_docs(self, context, flowfile, documents):
        from langchain.text_splitter import CharacterTextSplitter
        from langchain.text_splitter import RecursiveCharacterTextSplitter

        strategy = context.getProperty(self.CHUNK_STRATEGY).getValue()
        if strategy == SPLIT_BY_CHARACTER:
            text_splitter = CharacterTextSplitter(
                separator = context.getProperty(self.SEPARATOR).evaluateAttributeExpressions(flowfile).getValue(),
                keep_separator = context.getProperty(self.KEEP_SEPARATOR).asBoolean(),
                is_separator_regex = context.getProperty(self.SEPARATOR_FORMAT).getValue() == 'Regular Expression',
                chunk_size = context.getProperty(self.CHUNK_SIZE).asInteger(),
                chunk_overlap = context.getProperty(self.CHUNK_OVERLAP).asInteger(),
                length_function = len,
                strip_whitespace = context.getProperty(self.STRIP_WHITESPACE).asBoolean()
            )
        elif strategy == SPLIT_CODE:
            text_splitter = RecursiveCharacterTextSplitter.from_language(
                language=context.getProperty(self.LANGUAGE).getValue(),
                chunk_size = context.getProperty(self.CHUNK_SIZE).asInteger(),
                chunk_overlap = context.getProperty(self.CHUNK_OVERLAP).asInteger()
            )
        else:
            separator_text = context.getProperty(self.SEPARATOR).evaluateAttributeExpressions(flowfile).getValue()
            splits = separator_text.split(",")
            unescaped = []
            for split in splits:
                unescaped.append(split.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t"))
            text_splitter = RecursiveCharacterTextSplitter(
                separators = unescaped,
                keep_separator = context.getProperty(self.KEEP_SEPARATOR).asBoolean(),
                is_separator_regex = context.getProperty(self.SEPARATOR_FORMAT).getValue() == 'Regular Expression',
                chunk_size = context.getProperty(self.CHUNK_SIZE).asInteger(),
                chunk_overlap = context.getProperty(self.CHUNK_OVERLAP).asInteger(),
                length_function = len,
                strip_whitespace = context.getProperty(self.STRIP_WHITESPACE).asBoolean()
            )

        splits = text_splitter.split_documents(documents)
        return splits


    def to_json(self, docs) -> str:
        json_docs = []

        i = 0
        for doc in docs:
            doc.metadata['chunk_index'] = i
            doc.metadata['chunk_count'] = len(docs)
            i += 1

            json_doc = json.dumps({
                TEXT_KEY: doc.page_content,
                METADATA_KEY: doc.metadata
            })
            json_docs.append(json_doc)

        return "\n".join(json_docs)


    def load_docs(self, flowfile):
        from langchain.schema import Document

        flowfile_contents = flowfile.getContentsAsBytes().decode()
        docs = []
        for line in flowfile_contents.split("\n"):
            stripped = line.strip()
            if stripped == "":
                continue

            json_element = json.loads(stripped)
            page_content = json_element.get(TEXT_KEY)
            if page_content is None:
                continue

            metadata = json_element.get(METADATA_KEY)
            if metadata is None:
                metadata = {}

            doc = Document(page_content=page_content, metadata=metadata)
            docs.append(doc)

        return docs


    def transform(self, context, flowfile):
        documents = self.load_docs(flowfile)
        split_docs = self.split_docs(context, flowfile, documents)

        output_json = self.to_json(split_docs)
        attributes = {"document.count": str(len(split_docs))}
        return FlowFileTransformResult("success", contents=output_json, attributes=attributes)
