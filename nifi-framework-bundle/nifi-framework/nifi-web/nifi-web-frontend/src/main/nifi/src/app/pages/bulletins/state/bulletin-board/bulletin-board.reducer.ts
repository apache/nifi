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

import { BulletinBoardEvent, BulletinBoardItem, BulletinBoardState } from './index';
import { createReducer, on } from '@ngrx/store';
import {
    clearBulletinBoard,
    loadBulletinBoard,
    loadBulletinBoardSuccess,
    resetBulletinBoardState,
    setBulletinBoardAutoRefresh,
    setBulletinBoardFilter
} from './bulletin-board.actions';

export const initialBulletinBoardState: BulletinBoardState = {
    bulletinBoardItems: [],
    filter: {
        filterTerm: '',
        filterColumn: 'message'
    },
    autoRefresh: true,
    lastBulletinId: 0,
    status: 'pending',
    loadedTimestamp: ''
};

export const bulletinBoardReducer = createReducer(
    initialBulletinBoardState,

    on(loadBulletinBoard, (state: BulletinBoardState) => ({
        ...state,
        status: 'loading' as const
    })),

    on(loadBulletinBoardSuccess, (state: BulletinBoardState, { response }) => {
        // as bulletins are loaded, we just add them to the existing bulletins in the store
        const items = response.bulletinBoard.bulletins.map((bulletin) => {
            const bulletinItem: BulletinBoardItem = {
                item: bulletin
            };
            return bulletinItem;
        });

        let lastId = state.lastBulletinId;
        if (response.bulletinBoard.bulletins.length > 0) {
            lastId = response.bulletinBoard.bulletins[response.bulletinBoard.bulletins.length - 1].id;
        }

        return {
            ...state,
            bulletinBoardItems: [...state.bulletinBoardItems, ...items],
            lastBulletinId: lastId,
            status: 'success' as const,
            loadedTimestamp: response.loadedTimestamp
        };
    }),

    on(resetBulletinBoardState, () => ({ ...initialBulletinBoardState })),

    on(clearBulletinBoard, (state) => ({
        ...state,
        bulletinBoardItems: []
    })),

    on(setBulletinBoardFilter, (state: BulletinBoardState, { filter }) => {
        // add a new bulletin event to the list for the filter
        const event: BulletinBoardEvent = {
            type: 'filter',
            message:
                filter.filterTerm.length > 0
                    ? `Filter by ${filter.filterColumn} matching '${filter.filterTerm}'`
                    : 'Filter removed'
        };
        const item: BulletinBoardItem = { item: event };
        return {
            ...state,
            bulletinBoardItems: [...state.bulletinBoardItems, item],
            filter: { ...filter }
        };
    }),

    on(setBulletinBoardAutoRefresh, (state: BulletinBoardState, { autoRefresh }) => {
        const event: BulletinBoardEvent = {
            type: 'auto-refresh',
            message: autoRefresh ? `Auto-refresh started` : 'Auto-refresh stopped'
        };
        const item: BulletinBoardItem = { item: event };
        return {
            ...state,
            bulletinBoardItems: [...state.bulletinBoardItems, item],
            autoRefresh
        };
    })
);
