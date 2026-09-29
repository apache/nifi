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

import { Bundle, Permissions } from '../rest-api.types';
import { ComponentDTO, EmbeddedComponentEntityBase } from './component-entity';
import { PropertyDescriptorDTO } from './property-descriptor-dto';

/**
 * @nifi-source: nifi-framework-bundle/nifi-framework/nifi-client-dto/src/main/java/org/apache/nifi/web/api/dto/ControllerServiceDTO.java
 * @nifi-revision: eefa952edddc (2026-09-29)
 * @vetted: 2026-09-29
 */
export interface ControllerServiceDTO extends ComponentDTO {
    name: string;
    type: string;
    bundle: Bundle;
    controllerServiceApis: ControllerServiceApiDTO[];
    state: string;
    bulletinLevel: string;
    persistsState: boolean;
    restricted: boolean;
    deprecated: boolean;
    extensionMissing: boolean;
    multipleVersionsAvailable: boolean;
    supportsSensitiveDynamicProperties: boolean;
    properties: Record<string, string | null>;
    descriptors: Record<string, PropertyDescriptorDTO>;
    validationStatus: string;
    comments?: string;
    annotationData?: string;
    customUiUrl?: string;
    sensitiveDynamicPropertyNames?: string[];
    referencingComponents?: ControllerServiceReferencingComponentEntity[];
    validationErrors?: string[];
}

export interface ControllerServiceApiDTO {
    type: string;
    bundle: Bundle;
}

export interface ControllerServiceReferencingComponentEntity extends EmbeddedComponentEntityBase<ControllerServiceReferencingComponentDTO> {
    operatePermissions: Permissions;
}

export interface ControllerServiceReferencingComponentDTO {
    id: string;
    groupId?: string;
    name: string;
    type?: string;
    state?: string;
    properties: Record<string, string | null>;
    descriptors: Record<string, PropertyDescriptorDTO>;
    validationErrors?: string[];
    referenceType?: string;
    activeThreadCount?: number;
    referenceCycle?: boolean;
    referencingComponents?: ControllerServiceReferencingComponentEntity[];
}
