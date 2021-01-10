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

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as an AWS S3 directory (skipping the object name).
 * The analyzer outputs a v1 or v2 AWS S3 directory entity depending on the 'AWS S3 Model Version' property configured on the reporting task.
 * <p>
 * Atlas entity hierarchy v1: aws_s3_pseudo_dir -> aws_s3_bucket
 * <p>aws_s3_pseudo_dir
 * <ul>
 *   <li>qualifiedName=s3a://bucket/path@namespace (example: s3a://mybucket/mydir1/mydir2@ns1)
 *   <li>name=/path (example: /mydir1/mydir2)
 * </ul>
 * <p>aws_s3_bucket
 * <ul>
 *   <li>qualifiedName=s3a://bucket@namespace (example: s3a://mybucket@ns1)
 *   <li>name=bucket (example: mybucket)
 * </ul>
 * <p>
 * Atlas entity hierarchy v2: aws_s3_v2_directory -> aws_s3_v2_directory -> ... -> aws_s3_v2_bucket
 * <p>aws_s3_v2_directory
 * <ul>
 *   <li>qualifiedName=s3a://bucket/path/@namespace (example: s3a://mybucket/mydir1/mydir2/@ns1)
 *   <li>name=directory (example: mydir2)
 * </ul>
 * <p>aws_s3_v2_bucket
 * <ul>
 *   <li>qualifiedName=s3a://bucket@namespace (example: s3a://mybucket@ns1)
 *   <li>name=bucket (example: mybucket)
 * </ul>
 */
public class AwsS3Directory extends AbstractNiFiProvenanceEventAnalyzer {

    public static final String TYPE_DIRECTORY_V1 = AtlasPathExtractorUtil.AWS_S3_PSEUDO_DIR;
    public static final String TYPE_BUCKET_V1 = AtlasPathExtractorUtil.AWS_S3_BUCKET;
    public static final String ATTR_BUCKET_V1 = AtlasPathExtractorUtil.ATTRIBUTE_BUCKET;
    public static final String ATTR_OBJECT_PREFIX_V1 = AtlasPathExtractorUtil.ATTRIBUTE_OBJECT_PREFIX;

    public static final String TYPE_DIRECTORY_V2 = AtlasPathExtractorUtil.AWS_S3_V2_PSEUDO_DIR;
    public static final String TYPE_BUCKET_V2 = AtlasPathExtractorUtil.AWS_S3_V2_BUCKET;
    public static final String ATTR_CONTAINER_V2 = AtlasPathExtractorUtil.ATTRIBUTE_CONTAINER;
    public static final String ATTR_OBJECT_PREFIX_V2 = AtlasPathExtractorUtil.ATTRIBUTE_OBJECT_PREFIX;

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        String transitUri = event.getTransitUri();
        if (transitUri == null) {
            return null;
        }

        String directoryUri = transitUri.substring(0, transitUri.lastIndexOf('/') + 1);

        Path path = new Path(directoryUri);

        String namespace = context.getNamespaceResolver().fromHostNames(path.toUri().getHost());

        PathExtractorContext pathExtractorContext = new PathExtractorContext(namespace, context.getAwsS3ModelVersion());
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = AtlasPathExtractorUtil.getPathEntity(path, pathExtractorContext);

        Referenceable ref = convertToReferenceable(entityWithExtInfo.getEntity(), pathExtractorContext.getKnownEntities());

        return ref != null ? singleDataSetRef(event.getComponentId(), event.getEventType(), ref) : null;
    }

    @Override
    public String targetTransitUriPattern() {
        return "^s3a://.+/.+$";
    }

    private Referenceable convertToReferenceable(AtlasEntity entity, Map<String, AtlasEntity> knownEntities) {
        if (entity == null) {
            return null;
        }

        Referenceable ref = createReferenceable(entity);

        if (TYPE_DIRECTORY_V1.equals(entity.getTypeName())) {
            AtlasObjectId bucketObjectId = (AtlasObjectId) entity.getRelationshipAttribute(ATTR_BUCKET_V1);
            if (bucketObjectId != null) {
                AtlasEntity bucketEntity = knownEntities.get(bucketObjectId.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
                ref.set(ATTR_BUCKET_V1, convertToReferenceable(bucketEntity, knownEntities));
            }
        } else if (TYPE_DIRECTORY_V2.equals(entity.getTypeName())) {
            AtlasObjectId containerObjectId = (AtlasObjectId) entity.getRelationshipAttribute(ATTR_CONTAINER_V2);
            if (containerObjectId != null) {
                AtlasEntity containerEntity = knownEntities.get(containerObjectId.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));
                ref.set(ATTR_CONTAINER_V2, convertToReferenceable(containerEntity, knownEntities));
            }
        }

        return ref;
    }

    private Referenceable createReferenceable(AtlasEntity entity) {
        Referenceable ref = new Referenceable(entity.getTypeName());
        ref.setValues(entity.getAttributes());
        return ref;
    }

}
