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

import { createReducer, on } from '@ngrx/store';
import { NavigationState } from './index';
import { popBackNavigation, popBackNavigationByRouteBoundary, pushBackNavigation } from './navigation.actions';
import { produce } from 'immer';

export const initialState: NavigationState = {
    backNavigations: []
};

export const navigationReducer = createReducer(
    initialState,
    on(pushBackNavigation, (state, { backNavigation }) => {
        return produce(state, (draftState) => {
            if (draftState.backNavigations.length > 0) {
                const currentBackNavigation = draftState.backNavigations[draftState.backNavigations.length - 1];

                // don't push multiple back navigations going to the same route
                if (routesNotEqual(currentBackNavigation.route, backNavigation.route)) {
                    draftState.backNavigations.push(backNavigation);
                }
            } else {
                draftState.backNavigations.push(backNavigation);
            }
        });
    }),
    on(popBackNavigationByRouteBoundary, (state, { url }) => {
        return produce(state, (draftState) => {
            // pop any back navigation that is outside the bounds of the current url
            while (draftState.backNavigations.length > 0) {
                const lastBackNavigation = draftState.backNavigations[draftState.backNavigations.length - 1];
                if (!url.startsWith(lastBackNavigation.routeBoundary.join('/'))) {
                    draftState.backNavigations.pop();
                } else {
                    break;
                }
            }
        });
    }),
    on(popBackNavigation, (state) => {
        return produce(state, (draftState) => {
            if (draftState.backNavigations.length > 0) {
                draftState.backNavigations.pop();
            }
        });
    })
);

function routesNotEqual(route1: string[], route2: string[]) {
    if (route1.length !== route2.length) {
        return true;
    }

    for (let i = 0; i < route1.length; i++) {
        if (route1[i] !== route2[i]) {
            return true;
        }
    }

    return false;
}
