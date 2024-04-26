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

import { ActivatedRouteSnapshot, ResolveFn } from '@angular/router';
import { inject } from '@angular/core';
import { catchError, EMPTY, map, of, switchMap, take } from 'rxjs';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectProcessor } from '../../state/flow/flow.selectors';
import { FlowService } from '../flow.service';
import { AdvancedUiParams } from '../../../../state/shared';
import { Client } from '../../../../service/client.service';
import { fullScreenError } from '../../../../state/error/error.actions';
import { HttpErrorResponse } from '@angular/common/http';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';

export const processorAdvancedUiParamsResolver: ResolveFn<AdvancedUiParams> = (route: ActivatedRouteSnapshot) => {
    const store: Store<NiFiState> = inject(Store);
    const flowService: FlowService = inject(FlowService);
    const client: Client = inject(Client);
    const clusterConnectionService: ClusterConnectionService = inject(ClusterConnectionService);

    // getting id parameter from activated route because ngrx router store
    // is not initialized when this resolver executes
    const id: string | null = route.paramMap.get('id');
    if (!id) {
        return EMPTY;
    }

    return store.select(selectProcessor(id)).pipe(
        switchMap((processor) => {
            if (processor) {
                return of(processor);
            } else {
                return flowService.getProcessor(id).pipe(
                    catchError((errorResponse: HttpErrorResponse) => {
                        store.dispatch(
                            fullScreenError({
                                errorDetail: {
                                    title: 'Unable to Open Advanced UI',
                                    message: errorResponse.error
                                }
                            })
                        );
                        return EMPTY;
                    })
                );
            }
        }),
        map((entity) => {
            const revision = client.getRevision(entity);

            const editable = !(
                entity.status.aggregateSnapshot.runStatus === 'Running' ||
                entity.status.aggregateSnapshot.activeThreadCount > 0
            );

            return {
                url: entity.component.config.customUiUrl,
                id: entity.id,
                clientId: revision.clientId,
                revision: revision.version,
                editable,
                disconnectedNodeAcknowledged: clusterConnectionService.isDisconnectionAcknowledged()
            } as AdvancedUiParams;
        }),
        take(1)
    );
};
