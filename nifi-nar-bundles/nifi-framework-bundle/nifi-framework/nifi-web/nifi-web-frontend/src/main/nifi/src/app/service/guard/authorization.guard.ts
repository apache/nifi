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

import { CanMatchFn, Route, Router, UrlSegment } from '@angular/router';
import { inject } from '@angular/core';
import { map } from 'rxjs';
import { Store } from '@ngrx/store';
import { User, UserState } from '../../state/user';
import { selectUser } from '../../state/user/user.selectors';

export const authorizationGuard = (authorizationCheck: (user: User) => boolean): CanMatchFn => {
    return (route: Route, state: UrlSegment[]) => {
        const router: Router = inject(Router);
        const store: Store<UserState> = inject(Store<UserState>);

        return store.select(selectUser).pipe(
            map((currentUser) => {
                if (authorizationCheck(currentUser)) {
                    return true;
                }

                // TODO - replace with 404 error page
                return router.parseUrl('/');
            })
        );
    };
};
