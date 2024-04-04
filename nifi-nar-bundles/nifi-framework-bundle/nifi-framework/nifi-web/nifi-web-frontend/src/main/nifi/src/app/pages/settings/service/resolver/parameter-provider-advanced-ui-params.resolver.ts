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
import { AdvancedUiParams } from '../../../../state/shared';
import { Client } from '../../../../service/client.service';
import { HttpErrorResponse } from '@angular/common/http';
import { fullScreenError } from '../../../../state/error/error.actions';
import { ParameterProviderService } from '../parameter-provider.service';
import { selectParameterProvider } from '../../state/parameter-providers/parameter-providers.selectors';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';

export const parameterProviderAdvancedUiParamsResolver: ResolveFn<AdvancedUiParams> = (
    route: ActivatedRouteSnapshot
) => {
    const store: Store<NiFiState> = inject(Store);
    const parameterProviderService: ParameterProviderService = inject(ParameterProviderService);
    const client: Client = inject(Client);
    const clusterConnectionService: ClusterConnectionService = inject(ClusterConnectionService);

    // getting id parameter from activated route because ngrx router store
    // is not initialized when this resolver executes
    const id: string | null = route.paramMap.get('id');
    if (!id) {
        return EMPTY;
    }

    return store.select(selectParameterProvider(id)).pipe(
        switchMap((parameterProvider) => {
            if (parameterProvider) {
                return of(parameterProvider);
            } else {
                return parameterProviderService.getParameterProvider(id).pipe(
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

            return {
                url: entity.component.customUiUrl,
                id: entity.id,
                clientId: revision.clientId,
                revision: revision.version,
                editable: true,
                disconnectedNodeAcknowledged: clusterConnectionService.isDisconnectionAcknowledged()
            } as AdvancedUiParams;
        }),
        take(1)
    );
};
