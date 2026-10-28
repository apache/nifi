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
import { ActivatedRouteSnapshot, CanActivateFn, Router, UrlTree } from '@angular/router';
import { Store } from '@ngrx/store';
import { ConnectorEntity } from '@nifi/shared';
import { catchError, map, of, switchMap, take } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { ConnectorService } from '../../../service/connector.service';
import { ErrorHelper } from '../../../../../service/error-helper.service';
import {
    loadConnectorEntityFailure,
    loadConnectorEntitySuccess
} from '../../../state/connector-canvas-entity/connector-canvas-entity.actions';
import { selectConnectorCanvasEntity } from '../../../state/connector-canvas-entity/connector-canvas-entity.selectors';

export type TroubleshootingDenyRedirect = 'canvas' | 'controller-services';

export const connectorTroubleshootingGuard: CanActivateFn = (route: ActivatedRouteSnapshot) => {
    const store = inject(Store);
    const router = inject(Router);
    const connectorService = inject(ConnectorService);
    const errorHelper = inject(ErrorHelper);
    const connectorId = route.paramMap.get('id');
    const processGroupId = route.paramMap.get('processGroupId');
    const redirectKind =
        (route.data['troubleshootingDenyRedirect'] as TroubleshootingDenyRedirect | undefined) ?? 'canvas';

    const redirect = (): UrlTree => {
        const commands: Array<string | null> = ['/connectors', connectorId, 'canvas', processGroupId];
        if (redirectKind === 'controller-services') {
            commands.push('controller-services');
        }
        return router.createUrlTree(commands);
    };

    if (!connectorId || !processGroupId) {
        return redirect();
    }

    return store.select(selectConnectorCanvasEntity).pipe(
        take(1),
        switchMap((cached: ConnectorEntity | null) => {
            if (cached?.id === connectorId) {
                return of(cached);
            }
            return connectorService.getConnector(connectorId).pipe(
                map((connectorEntity) => {
                    store.dispatch(loadConnectorEntitySuccess({ connectorEntity }));
                    return connectorEntity;
                }),
                catchError((error: HttpErrorResponse) => {
                    store.dispatch(loadConnectorEntityFailure({ error: errorHelper.getErrorString(error) }));
                    return of(null);
                })
            );
        }),
        map((entity) => (entity?.component.state === 'TROUBLESHOOTING' ? true : redirect()))
    );
};
