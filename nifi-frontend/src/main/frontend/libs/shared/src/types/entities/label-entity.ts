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

import { PositionableComponentDTO, PositionableComponentEntityBase } from './component-entity';

/**
 * @nifi-source: nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/entity/LabelEntity.java
 * @nifi-revision: eefa952edddc (2026-09-29)
 * @vetted: 2026-09-29
 */
export interface LabelEntity extends PositionableComponentEntityBase<LabelDTO> {
    dimensions: DimensionsDTO;
    zIndex: number;
}

export interface LabelDTO extends PositionableComponentDTO {
    label?: string;
    style: Record<string, string>;
    width: number;
    height: number;
    zIndex: number;
}

export interface DimensionsDTO {
    width: number;
    height: number;
}
