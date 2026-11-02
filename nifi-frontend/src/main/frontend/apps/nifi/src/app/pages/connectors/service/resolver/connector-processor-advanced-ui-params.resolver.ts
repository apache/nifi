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

import { inject } from '@angular/core';
import { ActivatedRouteSnapshot, ResolveFn } from '@angular/router';
import { Store } from '@ngrx/store';
import { catchError, EMPTY, map, of, switchMap, take } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { AdvancedUiParams } from '../../../../state/shared';
import { FlowService } from '../../../flow-designer/service/flow.service';
import { Client } from '../../../../service/client.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { fullScreenError } from '../../../../state/error/error.actions';
import { selectProcessor } from '../../state/connector-canvas/connector-canvas.selectors';

export const connectorProcessorAdvancedUiParamsResolver: ResolveFn<AdvancedUiParams> = (
    route: ActivatedRouteSnapshot
) => {
    const store = inject(Store);
    const flowService = inject(FlowService);
    const client = inject(Client);
    const clusterConnectionService = inject(ClusterConnectionService);
    const errorHelper = inject(ErrorHelper);
    const id = route.paramMap.get('processorId');
    const processGroupId = route.paramMap.get('processGroupId');
    if (!id || !processGroupId) {
        return EMPTY;
    }

    return store.select(selectProcessor(id)).pipe(
        switchMap((processor) => (processor ? of(processor) : flowService.getProcessor(id))),
        map((entity) => {
            const customUiUrl = entity.component?.config?.customUiUrl;
            if (
                entity.component?.parentGroupId !== processGroupId ||
                entity.component?.extensionMissing === true ||
                !customUiUrl
            ) {
                throw new Error('Processor does not provide an Advanced UI');
            }
            const revision = client.getRevision(entity);
            return {
                url: customUiUrl,
                id: entity.id,
                clientId: revision.clientId,
                revision: revision.version,
                editable:
                    entity.permissions.canWrite &&
                    entity.status.aggregateSnapshot.runStatus !== 'Running' &&
                    entity.status.aggregateSnapshot.activeThreadCount === 0,
                disconnectedNodeAcknowledged: clusterConnectionService.isDisconnectionAcknowledged()
            };
        }),
        catchError((error: HttpErrorResponse | Error) => {
            store.dispatch(
                fullScreenError({
                    errorDetail: {
                        title: 'Unable to Open Advanced UI',
                        message: error instanceof HttpErrorResponse ? errorHelper.getErrorString(error) : error.message
                    }
                })
            );
            return EMPTY;
        }),
        take(1)
    );
};
