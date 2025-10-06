import { createSelector } from '@reduxjs/toolkit';

import type { RootState } from './store';
import type { NavigationState } from './navigationSlice';

export const selectNavigationState = (state: RootState): NavigationState => state.navigation;

export const selectBackNavigation = createSelector(selectNavigationState, (state) => {
  if (state.backNavigations.length === 0) {
    return undefined;
  }

  return state.backNavigations[state.backNavigations.length - 1];
});
