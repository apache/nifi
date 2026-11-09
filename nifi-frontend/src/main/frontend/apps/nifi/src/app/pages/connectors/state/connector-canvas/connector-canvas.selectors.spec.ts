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

import { ComponentType, ProcessorEntity } from '@nifi/shared';
import { selectConnectorCanvasSelectedComponents } from './connector-canvas.selectors';

function processor(id: string): ProcessorEntity {
    return {
        id,
        uri: `/processors/${id}`,
        revision: { version: 1 },
        permissions: { canRead: true, canWrite: true },
        position: { x: 0, y: 0 },
        status: {
            aggregateSnapshot: {
                id,
                groupId: 'pg-1',
                name: id,
                runStatus: 'Stopped',
                activeThreadCount: 0,
                terminatedThreadCount: 0
            }
        }
    } as ProcessorEntity;
}

function project(params: Record<string, string> | null, processors: ProcessorEntity[] = []) {
    return selectConnectorCanvasSelectedComponents.projector(params, processors, [], [], [], [], [], [], []);
}

describe('selectConnectorCanvasSelectedComponents', () => {
    it('resolves a deep-linked single selection from route parameters', () => {
        const target = processor('processor-1');

        const result = project({ type: ComponentType.Processor, componentId: target.id }, [target]);

        expect(result).toEqual([{ entity: target, ui: { componentType: ComponentType.Processor } }]);
    });

    it('preserves route order for a bulk selection and ignores unknown ids', () => {
        const first = processor('processor-1');
        const second = processor('processor-2');

        const result = project({ ids: `${second.id},missing,${first.id}` }, [first, second]);

        expect(result.map((target) => target.entity.id)).toEqual([second.id, first.id]);
    });

    it('returns an empty selection when the route has no selected component parameters', () => {
        expect(project({ processGroupId: 'pg-1' }, [processor('processor-1')])).toEqual([]);
    });
});
