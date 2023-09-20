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
from typing import Tuple

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope, PropertyDependency
import json

ROW_ORIENTED = "Row-Oriented"
TEXT = "Text"
COLUMN_ORIENTED = "Column-Oriented"


OUTPUT_STRATEGY = PropertyDescriptor(
    name="Output Strategy",
    description="Specifies whether the output should contain only the text of the documents (each document separated by \\n\\n), or if it " +
                "should be formatted as either single column-oriented JSON object, " +
                "consisting of a keys 'ids', 'embeddings', 'documents', 'distances', and 'metadatas'; or if the results should be row-oriented, " +
                "a JSON per line, each consisting of a single id, document, metadata, embedding, and distance.",
    allowable_values=[ROW_ORIENTED, TEXT, COLUMN_ORIENTED],
    default_value=ROW_ORIENTED,
    required=True
)
RESULTS_FIELD = PropertyDescriptor(
    name="Results Field",
    description="If the input FlowFile is JSON Formatted, this represents the name of the field to insert the results. This allows the results to be inserted into " +
                "an existing input in order to enrich it. If this property is unset, the results will be written to the FlowFile contents, overwriting any pre-existing content.",
    validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    required=False
)

INCLUDE_IDS = PropertyDescriptor(
    name="Include Document IDs",
    description="Whether or not to include the Documents' IDs in the response",
    allowable_values=["true", "false"],
    default_value="true",
    required=False,
    dependencies=[PropertyDependency(OUTPUT_STRATEGY, ROW_ORIENTED, COLUMN_ORIENTED)]
)
INCLUDE_METADATAS = PropertyDescriptor(
    name="Include Metadata",
    description="Whether or not to include the Documents' Metadata in the response",
    allowable_values=["true", "false"],
    default_value="true",
    required=False,
    dependencies=[PropertyDependency(OUTPUT_STRATEGY, ROW_ORIENTED, COLUMN_ORIENTED)]
)
INCLUDE_DOCUMENTS = PropertyDescriptor(
    name="Include Document",
    description="Whether or not to include the Documents' Text in the response",
    allowable_values=["true", "false"],
    default_value="true",
    required=False,
    dependencies=[PropertyDependency(OUTPUT_STRATEGY, ROW_ORIENTED, COLUMN_ORIENTED)]
)
INCLUDE_DISTANCES = PropertyDescriptor(
    name="Include Distances",
    description="Whether or not to include the Documents' Distances (i.e., how far the Document was away from the query) in the response",
    allowable_values=["true", "false"],
    default_value="true",
    required=False,
    dependencies=[PropertyDependency(OUTPUT_STRATEGY, ROW_ORIENTED, COLUMN_ORIENTED)]
)
INCLUDE_EMBEDDINGS = PropertyDescriptor(
    name="Include Embeddings",
    description="Whether or not to include the Documents' Embeddings in the response",
    allowable_values=["true", "false"],
    default_value="false",
    required=False,
    dependencies=[PropertyDependency(OUTPUT_STRATEGY, ROW_ORIENTED, COLUMN_ORIENTED)]
)


class QueryUtils:
    context = None

    def __init__(self, context):
        self.context = context
        self.results_field = context.getProperty(RESULTS_FIELD).getValue()
        self.output_strategy = context.getProperty(OUTPUT_STRATEGY).getValue()

        ids_property = context.getProperty(INCLUDE_IDS)
        self.include_ids = ids_property.asBoolean() if ids_property else False

        embeddings_property = context.getProperty(INCLUDE_EMBEDDINGS)
        self.include_embeddings = embeddings_property.asBoolean() if embeddings_property else False

        self.include_distances = context.getProperty(INCLUDE_DISTANCES).asBoolean()

        documents_property = context.getProperty(INCLUDE_DOCUMENTS)
        self.include_documents = documents_property.asBoolean() if documents_property else True
        self.include_metadatas = context.getProperty(INCLUDE_METADATAS).asBoolean()


    def create_json(self, flowfile, documents, metadatas, embeddings, distances, ids) -> Tuple[str, str]:
        if self.results_field is None:
            input_json = None
        else:
            input_json = json.loads(flowfile.getContentsAsBytes().decode())

        if self.output_strategy == TEXT:
            # Delete any document that is None or an empty-string
            documents = [doc for doc in documents if doc is not None and doc != ""]

            # Join the documents with two newlines
            text = "\n\n".join(documents)

            # Create either JSON or text output, based on whether or not an results field was specified
            if input_json is None:
                mime_type = "text/plain"
                output_contents = text
            else:
                input_json[self.results_field] = text
                output_contents = json.dumps(input_json)
                mime_type = "application/json"
        elif self.output_strategy == COLUMN_ORIENTED:
            doc = {}
            if self.include_ids:
                doc['ids'] = ids
            if self.include_distances:
                doc['distances'] = distances
            if self.include_documents:
                doc['documents'] = documents
            if self.include_metadatas:
                doc['metadatas'] = metadatas
            if self.include_embeddings:
                doc['embeddings'] = embeddings

            # Create the JSON from the Document
            if input_json is None:
                output_contents = json.dumps(doc)
            else:
                input_json[self.results_field] = doc
                output_contents = json.dumps(input_json)

            mime_type = "application/json"
        else:
            # Build the Documents
            docs = []

            count = len(ids) if ids else len(documents)
            for i in range(count):
                id = None if ids is None else ids[i]
                distance = None if distances is None else distances[i]
                metadata = None if metadatas is None else metadatas[i]
                document = None if documents is None else documents[i]
                embedding = None if embeddings is None else embeddings[i]

                # Create the document but do not include any key that we don't want to include in the output.
                doc = {}
                if self.include_ids:
                    doc['id'] = id
                if self.include_distances:
                    doc['distance'] = distance
                if self.include_documents:
                    doc['document'] = document
                if self.include_metadatas:
                    doc['metadata'] = metadata
                if self.include_embeddings:
                    doc['embedding'] = embedding

                docs.append(doc)

            # If input_json is None, we just create JSON based on the Documents.
            # If input_json is populated, we insert the documents into the input JSON using the specified key.
            if input_json is None:
                jsons = []
                for doc in docs:
                    jsons.append(json.dumps(doc))
                output_contents = "\n".join(jsons)
            else:
                input_json[self.results_field] = docs
                output_contents = json.dumps(input_json)

            mime_type = "application/json"

        return output_contents, mime_type
