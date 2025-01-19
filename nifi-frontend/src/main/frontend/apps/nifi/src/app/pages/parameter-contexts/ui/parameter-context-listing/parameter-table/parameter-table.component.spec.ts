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

import { ParameterItem, ParameterTable } from './parameter-table.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../state/parameter-context-listing/parameter-context-listing.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { EditParameterResponse } from '../../../../../state/shared';
import { Observable, of } from 'rxjs';
import { Parameter } from '@nifi/shared';

describe('ParameterTable', () => {
    let component: ParameterTable;
    let fixture: ComponentFixture<ParameterTable>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ParameterTable, NoopAnimationsModule],
            providers: [provideMockStore({ initialState })]
        });
        fixture = TestBed.createComponent(ParameterTable);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should handle no parameters with no edits', () => {
        component.writeValue([]);
        expect(component.serializeParameters()).toEqual([]);
    });

    it('should handle no parameters with one added', () => {
        component.writeValue([]);

        const parameter: Parameter = {
            name: 'param',
            value: 'value',
            description: 'asdf',
            sensitive: false
        };
        component.createNewParameter = (): Observable<EditParameterResponse> => {
            return of({
                parameter,
                valueChanged: true
            });
        };
        component.newParameterClicked();

        expect(component.serializeParameters()).toEqual([{ parameter }]);
    });

    it('should handle no parameters with one added and then edited', () => {
        component.writeValue([]);

        const parameter: Parameter = {
            name: 'param',
            value: 'value',
            description: 'asdf',
            sensitive: false
        };
        component.createNewParameter = (): Observable<EditParameterResponse> => {
            return of({
                parameter,
                valueChanged: true
            });
        };
        component.newParameterClicked();

        const parameterItem: ParameterItem = component.dataSource.data[0];
        expect(parameterItem.originalEntity.parameter).toEqual(parameter);

        const description = 'updated description';
        component.editParameter = (): Observable<EditParameterResponse> => {
            return of({
                parameter: {
                    name: 'param',
                    value: null,
                    sensitive: false,
                    description
                },
                valueChanged: false
            });
        };
        component.editClicked(parameterItem);

        expect(component.serializeParameters()).toEqual([
            {
                parameter: {
                    ...parameter,
                    description
                }
            }
        ]);
    });
});
