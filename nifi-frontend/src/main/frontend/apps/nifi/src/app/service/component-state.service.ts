/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { ClearComponentStateRequest, ComponentStateEntity, LoadComponentStateRequest } from '../state/component-state';
import { Observable } from 'rxjs';
import { ComponentType, NiFiCommon } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class ComponentStateService {
    private static readonly API = '../nifi-api';

    private httpClient = inject(HttpClient);
    private nifiCommon = inject(NiFiCommon);

    getComponentState(request: LoadComponentStateRequest): Observable<ComponentStateEntity> {
        const path = this.nifiCommon.getComponentTypeApiPath(request.componentType);
        return this.httpClient.get<ComponentStateEntity>(
            `${ComponentStateService.API}/${path}/${request.componentId}/state`
        );
    }

    clearComponentState(request: ClearComponentStateRequest): Observable<any> {
        const path = this.nifiCommon.getComponentTypeApiPath(request.componentType);
        return this.httpClient.post(
            `${ComponentStateService.API}/${path}/${request.componentId}/state/clear-requests`,
            {}
        );
    }

    clearComponentStateEntry(
        componentType: ComponentType,
        componentId: string,
        componentStateEntity: ComponentStateEntity
    ): Observable<any> {
        // To clear a specific state entry, we send the updated state
        // without the key to be cleared in the ComponentStateEntity format
        const path = this.nifiCommon.getComponentTypeApiPath(componentType);
        return this.httpClient.post(
            `${ComponentStateService.API}/${path}/${componentId}/state/clear-requests`,
            componentStateEntity
        );
    }
}
