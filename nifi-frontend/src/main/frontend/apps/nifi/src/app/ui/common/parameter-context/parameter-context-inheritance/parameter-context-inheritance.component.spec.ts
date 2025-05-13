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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ParameterContextInheritance } from './parameter-context-inheritance.component';
import { ParameterContextEntity } from '../../../../state/shared';

describe('ParameterContextInheritance', () => {
    let component: ParameterContextInheritance;
    let fixture: ComponentFixture<ParameterContextInheritance>;

    const parameterContext1: ParameterContextEntity = {
        revision: { version: 1 },
        uri: '',
        id: 'pc 1',
        permissions: { canRead: true, canWrite: true },
        component: {
            id: 'pc 1',
            parameters: [],
            name: 'parameter context 1',
            description: 'description',
            boundProcessGroups: [],
            inheritedParameterContexts: []
        }
    };

    const parameterContext2: ParameterContextEntity = {
        revision: { version: 1 },
        uri: '',
        id: 'pc 2',
        permissions: { canRead: true, canWrite: true },
        component: {
            id: 'pc 2',
            parameters: [],
            name: 'parameter context 2',
            description: 'description',
            boundProcessGroups: [],
            inheritedParameterContexts: []
        }
    };

    const parameterContext3: ParameterContextEntity = {
        revision: { version: 1 },
        uri: '',
        id: 'pc 3',
        permissions: { canRead: true, canWrite: true },
        component: {
            id: 'pc 3',
            parameters: [],
            name: 'parameter context 3',
            description: 'description',
            boundProcessGroups: [],
            inheritedParameterContexts: []
        }
    };

    const parameterContext4: ParameterContextEntity = {
        revision: { version: 1 },
        uri: '',
        id: 'pc 4',
        permissions: { canRead: true, canWrite: true },
        component: {
            id: 'pc 4',
            parameters: [],
            name: 'parameter context 4',
            description: 'description',
            boundProcessGroups: [],
            inheritedParameterContexts: []
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ParameterContextInheritance]
        });
        fixture = TestBed.createComponent(ParameterContextInheritance);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should retain the order of inheritance', () => {
        component.allParameterContexts = [parameterContext1, parameterContext2, parameterContext3, parameterContext4];
        // set the inheritance chain
        component.writeValue([parameterContext2, parameterContext1, parameterContext3]);
        fixture.detectChanges();

        expect(component.selectedParameterContexts.length).toEqual(3);
        expect(component.selectedParameterContexts[0].component?.id).toEqual('pc 2');
        expect(component.selectedParameterContexts[1].component?.id).toEqual('pc 1');
        expect(component.selectedParameterContexts[2].component?.id).toEqual('pc 3');
    });
});
