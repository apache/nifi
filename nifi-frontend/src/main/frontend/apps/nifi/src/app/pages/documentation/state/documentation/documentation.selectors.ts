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

import { createSelector } from '@ngrx/store';
import { DefinitionCoordinates } from '../index';
import { ComponentType, selectCurrentRoute } from '@nifi/shared';

export const selectOverviewFromRoute = createSelector(selectCurrentRoute, (route) => {
    const path = route.routeConfig.path;
    if (path.startsWith('overview')) {
        return true;
    }
    return null;
});

export const selectDefinitionCoordinatesFromRouteForComponentType = (componentType: ComponentType) =>
    createSelector(selectCurrentRoute, (route) => {
        let coordinates: DefinitionCoordinates | null = null;
        if (route?.params.group && route?.params.artifact && route?.params.version && route?.params.type) {
            const path = route.routeConfig.path;

            if (path.startsWith(componentType)) {
                coordinates = {
                    group: route.params.group,
                    artifact: route.params.artifact,
                    version: route.params.version,
                    type: route.params.type
                };
            }
        }
        return coordinates;
    });

export const selectDefinitionCoordinatesFromRoute = createSelector(selectCurrentRoute, (route) => {
    let coordinates: DefinitionCoordinates | null = null;
    if (route?.params.group && route?.params.artifact && route?.params.version && route?.params.type) {
        coordinates = {
            group: route.params.group,
            artifact: route.params.artifact,
            version: route.params.version,
            type: route.params.type
        };
    }
    return coordinates;
});
