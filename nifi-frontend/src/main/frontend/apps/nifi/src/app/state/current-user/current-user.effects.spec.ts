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

import { TestBed } from '@angular/core/testing';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { Action } from '@ngrx/store';
import { provideMockStore } from '@ngrx/store/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { Subject, of } from 'rxjs';
import { CurrentUserEffects } from './current-user.effects';
import { logout, navigateToLogOut, promptLogout } from './current-user.actions';
import { AuthService } from '../../service/auth.service';
import { CurrentUserService } from '../../service/current-user.service';
import { ErrorHelper } from '../../service/error-helper.service';

describe('Logout confirmation', () => {
    let actions: Subject<Action>;
    let closed: Subject<boolean | undefined>;
    let effects: CurrentUserEffects;
    const open = vi.fn();
    const nativeLogout = vi.fn();

    beforeEach(() => {
        actions = new Subject<Action>();
        closed = new Subject<boolean | undefined>();
        open.mockReset().mockReturnValue({ afterClosed: () => closed.asObservable() });
        nativeLogout.mockReset().mockReturnValue(of(undefined));
        TestBed.configureTestingModule({
            providers: [
                CurrentUserEffects,
                provideMockActions(() => actions),
                provideMockStore(),
                { provide: MatDialog, useValue: { open } },
                { provide: Router, useValue: { navigate: vi.fn() } },
                { provide: AuthService, useValue: { logout: nativeLogout } },
                { provide: CurrentUserService, useValue: { getUser: vi.fn() } },
                { provide: ErrorHelper, useValue: { fullScreenError: vi.fn() } }
            ]
        });
        effects = TestBed.inject(CurrentUserEffects);
    });

    afterEach(() => {
        actions.complete();
        closed.complete();
    });

    it.each([
        ['Cancel', false],
        ['dismissal', undefined],
        ['Confirm', true]
    ] as const)('requires explicit Confirm and preserves the session on %s', (_name, result) => {
        const emitted: Action[] = [];
        const subscription = effects.promptLogout$.subscribe((action) => emitted.push(action));
        actions.next(promptLogout());
        expect(open).toHaveBeenCalledOnce();
        expect(emitted).toEqual([]);
        expect(nativeLogout).not.toHaveBeenCalled();
        closed.next(result);
        closed.complete();
        expect(emitted).toEqual(result === true ? [logout()] : []);
        subscription.unsubscribe();
    });

    it('ignores repeated logout requests while the confirmation is open', () => {
        const subscription = effects.promptLogout$.subscribe();
        actions.next(promptLogout());
        actions.next(promptLogout());
        expect(open).toHaveBeenCalledOnce();
        subscription.unsubscribe();
    });

    it('retains the native logout and redirect action after confirmation', () => {
        const emitted: Action[] = [];
        const subscription = effects.logout$.subscribe((action) => emitted.push(action));
        actions.next(logout());
        expect(nativeLogout).toHaveBeenCalledOnce();
        expect(emitted).toEqual([navigateToLogOut()]);
        expect(open).not.toHaveBeenCalled();
        subscription.unsubscribe();
    });
});
