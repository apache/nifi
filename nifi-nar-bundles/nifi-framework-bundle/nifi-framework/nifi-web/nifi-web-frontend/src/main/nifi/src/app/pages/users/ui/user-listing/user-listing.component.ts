/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Component, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { selectUser } from '../../../../state/user/user.selectors';
import { UserListingState } from '../../state/user-listing';
import { selectUserListingState } from '../../state/user-listing/user-listing.selectors';
import { initialState } from '../../state/user-listing/user-listing.reducer';
import { loadTenants } from '../../state/user-listing/user-listing.actions';

@Component({
    selector: 'user-listing',
    templateUrl: './user-listing.component.html',
    styleUrls: ['./user-listing.component.scss']
})
export class UserListing implements OnInit {
    userListingState$ = this.store.select(selectUserListingState);
    currentUser$ = this.store.select(selectUser);

    constructor(private store: Store<UserListingState>) {}

    ngOnInit(): void {
        this.store.dispatch(loadTenants());
    }

    isInitialLoading(state: UserListingState): boolean {
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    refreshUserListing() {
        this.store.dispatch(loadTenants());
    }
}
