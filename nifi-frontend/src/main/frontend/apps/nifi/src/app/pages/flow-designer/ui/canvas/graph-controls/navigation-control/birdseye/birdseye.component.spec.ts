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

import { Birdseye } from './birdseye.component';
import { BirdseyeView } from '../../../../../service/birdseye-view.service';

describe('Birdseye', () => {
    let component: Birdseye;
    let fixture: ComponentFixture<Birdseye>;
    let birdseyeView: BirdseyeView;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [Birdseye],
            providers: [
                {
                    provide: BirdseyeView,
                    useValue: {
                        init: jest.fn(),
                        refresh: jest.fn(),
                        destroy: jest.fn()
                    }
                }
            ]
        });
        fixture = TestBed.createComponent(Birdseye);
        birdseyeView = TestBed.inject(BirdseyeView);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        const initSpy = jest.spyOn(birdseyeView, 'init');
        const refreshSpy = jest.spyOn(birdseyeView, 'refresh');
        expect(initSpy).toHaveBeenCalled();
        expect(refreshSpy).toHaveBeenCalled();
        expect(component).toBeTruthy();
    });
});
