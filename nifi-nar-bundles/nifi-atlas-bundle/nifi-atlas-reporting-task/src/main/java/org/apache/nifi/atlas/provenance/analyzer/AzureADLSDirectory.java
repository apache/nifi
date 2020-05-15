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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.atlas.utils.PathExtractorContext;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import java.util.Map;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as an Azure ADLS Gen2 directory (skipping the file name).
 * <li>qualifiedName=abfs://filesystem@account/path@namespace (example: abfs://myfilesystem@myaccount/mydir1/mydir2@ns1)
 * <li>name=directory (example: mydir2)
 */
public class AzureADLSDirectory extends AbstractNiFiProvenanceEventAnalyzer {

    public static final String TYPE_DIRECTORY = "adls_gen2_directory";
    public static final String TYPE_CONTAINER = "adls_gen2_container";
    public static final String TYPE_ACCOUNT = "adls_gen2_account";

    public static final String ATTR_PARENT = "parent";
    public static final String ATTR_ACCOUNT = "account";

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        String transitUri = event.getTransitUri();
        if (transitUri == null) {
            return null;
        }

        Path path = new Path(transitUri);

        String namespace = context.getNamespaceResolver().fromHostNames(path.toUri().getHost());

        PathExtractorContext pathExtractorContext = new PathExtractorContext(namespace);
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = AtlasPathExtractorUtil.getPathEntity(path, pathExtractorContext);

        // the last component of the URI is returned as a directory object but in fact it refers the filename
        Referenceable fileRef = convertToReferenceable(entityWithExtInfo.getEntity(), pathExtractorContext.getKnownEntities());
        Referenceable parentRef = (Referenceable) fileRef.get(ATTR_PARENT);

        return parentRef != null ? singleDataSetRef(event.getComponentId(), event.getEventType(), parentRef) : null;
    }

    @Override
    public String targetTransitUriPattern() {
        return "^abfs(s)?://.+@.+/.+$";
    }

    private Referenceable convertToReferenceable(AtlasEntity entity, Map<String, AtlasEntity> knownEntities) {
        if (entity == null) {
            return null;
        }

        Referenceable ref = new Referenceable(entity.getTypeName());

        ref.set(ATTR_QUALIFIED_NAME, entity.getAttribute(ATTR_QUALIFIED_NAME));
        ref.set(ATTR_NAME, entity.getAttribute(ATTR_NAME));

        if (TYPE_DIRECTORY.equals(entity.getTypeName())) {
            AtlasObjectId parentObjectId = (AtlasObjectId) entity.getRelationshipAttribute(ATTR_PARENT);
            if (parentObjectId != null) {
                AtlasEntity parentEntity = knownEntities.get(parentObjectId.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
                ref.set(ATTR_PARENT, convertToReferenceable(parentEntity, knownEntities));
            }
        } else if (TYPE_CONTAINER.equals(entity.getTypeName())) {
            AtlasObjectId accountObjectId = (AtlasObjectId) entity.getRelationshipAttribute(ATTR_ACCOUNT);
            if (accountObjectId != null) {
                AtlasEntity accountEntity = knownEntities.get(accountObjectId.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
                ref.set(ATTR_ACCOUNT, convertToReferenceable(accountEntity, knownEntities));
            }
        }

        return ref;
    }
}
