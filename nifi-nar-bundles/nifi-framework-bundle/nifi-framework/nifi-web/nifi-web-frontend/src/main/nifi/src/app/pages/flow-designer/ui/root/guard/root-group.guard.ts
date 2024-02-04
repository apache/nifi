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

import { CanActivateFn, Router } from '@angular/router';
import { FlowService } from '../../../service/flow.service';
import { inject } from '@angular/core';
import { switchMap, take } from 'rxjs';
import { Store } from '@ngrx/store';
import { CurrentUserState } from '../../../../../state/current-user';
import { FlowState } from '../../../state/flow';
import { selectCurrentProcessGroupId } from '../../../state/flow/flow.selectors';
import { initialState } from '../../../state/flow/flow.reducer';

export const rootGroupGuard: CanActivateFn = () => {
    const router: Router = inject(Router);
    const flowService: FlowService = inject(FlowService);
    const store: Store<CurrentUserState> = inject(Store<FlowState>);

    return store.select(selectCurrentProcessGroupId).pipe(
        take(1),
        switchMap((pgId) => {
            if (pgId == initialState.id) {
                return flowService.getProcessGroupStatus().pipe(
                    take(1),
                    switchMap((rootGroupStatus: any) => {
                        return router.navigate(['/process-groups', rootGroupStatus.processGroupStatus.id]);
                    })
                );
            } else {
                return router.navigate(['/process-groups', pgId]);
            }
        })
    );
};
