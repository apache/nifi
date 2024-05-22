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

from langchain.vectorstores import OpenSearchVectorSearch
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope, PropertyDependency
from OpenSearchVectorUtils import (L2, L1, LINF, COSINESIMIL, OPENAI_API_KEY, OPENAI_API_MODEL, HUGGING_FACE_API_KEY,
                                   HUGGING_FACE_MODEL, HTTP_HOST, USERNAME, PASSWORD, INDEX_NAME, VECTOR_FIELD,
                                   TEXT_FIELD, create_authentication_params, parse_documents)
from EmbeddingUtils import EMBEDDING_MODEL, create_embedding_service
from nifiapi.documentation import use_case, ProcessorConfiguration


@use_case(description="Create vectors/embeddings that represent text content and send the vectors to OpenSearch",
          notes="This use case assumes that the data has already been formatted in JSONL format with the text to store in OpenSearch provided in the 'text' field.",
          keywords=["opensearch", "embedding", "vector", "text", "vectorstore", "insert"],
          configuration="""
                Configure the 'HTTP Host' to an appropriate URL where OpenSearch is accessible.
                Configure 'Embedding Model' to indicate whether OpenAI embeddings should be used or a HuggingFace embedding model should be used: 'Hugging Face Model' or 'OpenAI Model'
                Configure the 'OpenAI API Key' or 'HuggingFace API Key', depending on the chosen Embedding Model.
                Set 'Index Name' to the name of your OpenSearch Index.
                Set 'Vector Field Name' to the name of the field in the document which will store the vector data.
                Set 'Text Field Name' to the name of the field in the document which will store the text data.

                If the documents to send to OpenSearch contain a unique identifier, set the 'Document ID Field Name' property to the name of the field that contains the document ID.
                This property can be left blank, in which case a unique ID will be generated based on the FlowFile's filename.

                If the provided index does not exists in OpenSearch then the processor is capable to create it. The 'New Index Strategy' property defines 
                that the index needs to be created from the default template or it should be configured with custom values.
                """)
@use_case(description="Update vectors/embeddings in OpenSearch",
          notes="This use case assumes that the data has already been formatted in JSONL format with the text to store in OpenSearch provided in the 'text' field.",
          keywords=["opensearch", "embedding", "vector", "text", "vectorstore", "update", "upsert"],
          configuration="""
                Configure the 'HTTP Host' to an appropriate URL where OpenSearch is accessible.
                Configure 'Embedding Model' to indicate whether OpenAI embeddings should be used or a HuggingFace embedding model should be used: 'Hugging Face Model' or 'OpenAI Model'
                Configure the 'OpenAI API Key' or 'HuggingFace API Key', depending on the chosen Embedding Model.
                Set 'Index Name' to the name of your OpenSearch Index.
                Set 'Vector Field Name' to the name of the field in the document which will store the vector data.
                Set 'Text Field Name' to the name of the field in the document which will store the text data.
                Set the 'Document ID Field Name' property to the name of the field that contains the identifier of the document in OpenSearch to update.
                """)
class PutOpenSearchVector(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '@project.version@'
        description = """Publishes JSON data to OpenSearch. The Incoming data must be in single JSON per Line format, each with two keys: 'text' and 'metadata'.
                       The text must be a string, while metadata must be a map with strings for values. Any additional fields will be ignored."""
        tags = ["opensearch", "vector", "vectordb", "vectorstore", "embeddings", "ai", "artificial intelligence", "ml",
                "machine learning", "text", "LLM"]

    # Engine types
    NMSLIB = ("nmslib (Non-Metric Space Library)", "nmslib")
    FAISS = ("faiss (Facebook AI Similarity Search)", "faiss")
    LUCENE = ("lucene", "lucene")

    ENGINE_VALUES = dict([NMSLIB, FAISS, LUCENE])

    # Space types
    INNERPRODUCT = ("Inner product", "innerproduct")

    NMSLIB_SPACE_TYPE_VALUES = dict([L2, L1, LINF, COSINESIMIL, INNERPRODUCT])
    FAISS_SPACE_TYPE_VALUES = dict([L2, INNERPRODUCT])
    LUCENE_SPACE_TYPE_VALUES = dict([L2, COSINESIMIL])

    # New Index Mapping Strategy
    DEFAULT_INDEX_MAPPING = "Default index mapping"
    CUSTOM_INDEX_MAPPING = "Custom index mapping"

    DOC_ID_FIELD_NAME = PropertyDescriptor(
        name="Document ID Field Name",
        description="""Specifies the name of the field in the 'metadata' element of each document where the document's ID can be found.  
                    If not specified, an ID will be generated based on the FlowFile's filename and a one-up number.""",
        required=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    NEW_INDEX_STRATEGY = PropertyDescriptor(
        name="New Index Strategy",
        description="Specifies the Mapping strategy to use for new index creation. The default template values are the following: "
                    "{engine: nmslib, space_type: l2, ef_search: 512, ef_construction: 512, m: 16}",
        allowable_values=[DEFAULT_INDEX_MAPPING, CUSTOM_INDEX_MAPPING],
        default_value=DEFAULT_INDEX_MAPPING,
        required=False,
    )
    ENGINE = PropertyDescriptor(
        name="Engine",
        description="The approximate k-NN library to use for indexing and search.",
        allowable_values=ENGINE_VALUES.keys(),
        default_value=NMSLIB[0],
        required=False,
        dependencies=[PropertyDependency(NEW_INDEX_STRATEGY, CUSTOM_INDEX_MAPPING)]
    )
    NMSLIB_SPACE_TYPE = PropertyDescriptor(
        name="NMSLIB Space Type",
        description="The vector space used to calculate the distance between vectors.",
        allowable_values=NMSLIB_SPACE_TYPE_VALUES.keys(),
        default_value=L2[0],
        required=False,
        dependencies=[PropertyDependency(NEW_INDEX_STRATEGY, CUSTOM_INDEX_MAPPING),
                      PropertyDependency(ENGINE, NMSLIB[0])]
    )
    FAISS_SPACE_TYPE = PropertyDescriptor(
        name="FAISS Space Type",
        description="The vector space used to calculate the distance between vectors.",
        allowable_values=FAISS_SPACE_TYPE_VALUES.keys(),
        default_value=L2[0],
        required=False,
        dependencies=[PropertyDependency(NEW_INDEX_STRATEGY, CUSTOM_INDEX_MAPPING),
                      PropertyDependency(ENGINE, FAISS[0])]
    )
    LUCENE_SPACE_TYPE = PropertyDescriptor(
        name="Lucene Space Type",
        description="The vector space used to calculate the distance between vectors.",
        allowable_values=LUCENE_SPACE_TYPE_VALUES.keys(),
        default_value=L2[0],
        required=False,
        dependencies=[PropertyDependency(NEW_INDEX_STRATEGY, CUSTOM_INDEX_MAPPING),
                      PropertyDependency(ENGINE, LUCENE[0])]
    )
    EF_SEARCH = PropertyDescriptor(
        name="EF Search",
        description="The size of the dynamic list used during k-NN searches. Higher values lead to more accurate but slower searches.",
        default_value="512",
        required=False,
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        dependencies=[PropertyDependency(NEW_INDEX_STRATEGY, CUSTOM_INDEX_MAPPING)]
    )
    EF_CONSTRUCTION = PropertyDescriptor(
        name="EF Construction",
        description="The size of the dynamic list used during k-NN graph creation. Higher values lead to a more accurate graph but slower indexing speed.",
        default_value="512",
        required=False,
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        dependencies=[PropertyDependency(NEW_INDEX_STRATEGY, CUSTOM_INDEX_MAPPING)]
    )
    M = PropertyDescriptor(
        name="M",
        description="The number of bidirectional links that the plugin creates for each new element. Increasing and "
                    "decreasing this value can have a large impact on memory consumption. Keep this value between 2 and 100.",
        default_value="16",
        required=False,
        validators=[StandardValidators._standard_validators.createLongValidator(2, 100, True)],
        dependencies=[PropertyDependency(NEW_INDEX_STRATEGY, CUSTOM_INDEX_MAPPING)]
    )

    properties = [EMBEDDING_MODEL,
                  OPENAI_API_KEY,
                  OPENAI_API_MODEL,
                  HUGGING_FACE_API_KEY,
                  HUGGING_FACE_MODEL,
                  HTTP_HOST,
                  USERNAME,
                  PASSWORD,
                  INDEX_NAME,
                  DOC_ID_FIELD_NAME,
                  VECTOR_FIELD,
                  TEXT_FIELD,
                  NEW_INDEX_STRATEGY,
                  ENGINE,
                  NMSLIB_SPACE_TYPE,
                  FAISS_SPACE_TYPE,
                  LUCENE_SPACE_TYPE,
                  EF_SEARCH,
                  EF_CONSTRUCTION,
                  M]

    embeddings = None

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def onScheduled(self, context):
        self.embeddings = create_embedding_service(context)

    def transform(self, context, flowfile):
        file_name = flowfile.getAttribute("filename")
        http_host = context.getProperty(HTTP_HOST).evaluateAttributeExpressions(flowfile).getValue()
        index_name = context.getProperty(INDEX_NAME).evaluateAttributeExpressions(flowfile).getValue()
        id_field_name = context.getProperty(self.DOC_ID_FIELD_NAME).evaluateAttributeExpressions(flowfile).getValue()
        vector_field = context.getProperty(VECTOR_FIELD).evaluateAttributeExpressions(flowfile).getValue()
        text_field = context.getProperty(TEXT_FIELD).evaluateAttributeExpressions(flowfile).getValue()
        new_index_strategy = context.getProperty(self.NEW_INDEX_STRATEGY).evaluateAttributeExpressions().getValue()

        params = {"vector_field": vector_field, "text_field": text_field}
        params.update(create_authentication_params(context))

        if new_index_strategy == self.CUSTOM_INDEX_MAPPING:
            engine = context.getProperty(self.ENGINE).evaluateAttributeExpressions().getValue()
            params["engine"] = self.ENGINE_VALUES.get(engine)

            if engine == self.NMSLIB[0]:
                space_type = context.getProperty(self.NMSLIB_SPACE_TYPE).evaluateAttributeExpressions().getValue()
                params["space_type"] = self.NMSLIB_SPACE_TYPE_VALUES.get(space_type)
            if engine == self.FAISS[0]:
                space_type = context.getProperty(self.FAISS_SPACE_TYPE).evaluateAttributeExpressions().getValue()
                params["space_type"] = self.FAISS_SPACE_TYPE_VALUES.get(space_type)
            if engine == self.LUCENE[0]:
                space_type = context.getProperty(self.LUCENE_SPACE_TYPE).evaluateAttributeExpressions().getValue()
                params["space_type"] = self.LUCENE_SPACE_TYPE_VALUES.get(space_type)

            ef_search = context.getProperty(self.EF_SEARCH).evaluateAttributeExpressions().asInteger()
            params["ef_search"] = ef_search

            ef_construction = context.getProperty(self.EF_CONSTRUCTION).evaluateAttributeExpressions().asInteger()
            params["ef_construction"] = ef_construction

            m = context.getProperty(self.M).evaluateAttributeExpressions().asInteger()
            params["m"] = m

        # Read the FlowFile content as "json-lines".
        json_lines = flowfile.getContentsAsBytes().decode()
        parsed_documents = parse_documents(json_lines, id_field_name, file_name)

        vectorstore = OpenSearchVectorSearch(
            opensearch_url=http_host,
            index_name=index_name,
            embedding_function=self.embeddings,
            **params
        )
        vectorstore.add_texts(texts=parsed_documents["texts"],
                              metadatas=parsed_documents["metadatas"],
                              ids=parsed_documents["ids"],
                              **params
                              )

        return FlowFileTransformResult(relationship="success")

