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

import { ActivatedRouteSnapshot } from '@angular/router';
import { ComponentType } from '@nifi/shared';
import { selectViewStatusHistoryComponent, selectViewStatusHistoryCurrentProcessGroup } from './flow.selectors';

function route(params: Record<string, string>, path = 'history'): ActivatedRouteSnapshot {
    return { params, routeConfig: { path } } as ActivatedRouteSnapshot;
}

describe('status history routing with inherited parameters', () => {
    it('opens only the group history for a group history URL', () => {
        const current = route({ processGroupId: 'group' });
        expect(selectViewStatusHistoryCurrentProcessGroup.projector(current)).toBe('group');
        expect(selectViewStatusHistoryComponent.projector(current)).toBeNull();
    });
    it.each([ComponentType.Connection, ComponentType.Processor, ComponentType.ProcessGroup])(
        'does not open parent history for a %s deep link',
        (type) => {
            const current = route({ processGroupId: 'parent', id: 'component', type });
            expect(selectViewStatusHistoryCurrentProcessGroup.projector(current)).toBeNull();
            expect(selectViewStatusHistoryComponent.projector(current)).toEqual({
                id: 'component',
                componentType: type
            });
        }
    );
    it('does not infer group history from incomplete component parameters or other routes', () => {
        expect(
            selectViewStatusHistoryCurrentProcessGroup.projector(
                route({ processGroupId: 'parent', type: ComponentType.Processor })
            )
        ).toBeNull();
        const current = route({ processGroupId: 'group' }, '');
        expect(selectViewStatusHistoryCurrentProcessGroup.projector(current)).toBeNull();
        expect(selectViewStatusHistoryComponent.projector(current)).toBeNull();
    });
});
