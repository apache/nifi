import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface BackNavigation {
  route: string[];
  routeBoundary: string[];
  context: string;
}

export interface NavigationState {
  backNavigations: BackNavigation[];
}

const initialState: NavigationState = {
  backNavigations: []
};

interface PushBackNavigationPayload {
  backNavigation: BackNavigation;
}

interface PopBackNavigationByRouteBoundaryPayload {
  url: string;
}

const navigationSlice = createSlice({
  name: 'navigation',
  initialState,
  reducers: {
    pushBackNavigation(state, action: PayloadAction<PushBackNavigationPayload>) {
      const { backNavigation } = action.payload;
      const currentBackNavigation = state.backNavigations[state.backNavigations.length - 1];

      if (!currentBackNavigation || routesNotEqual(currentBackNavigation.route, backNavigation.route)) {
        state.backNavigations.push(backNavigation);
      }
    },
    popBackNavigationByRouteBoundary(state, action: PayloadAction<PopBackNavigationByRouteBoundaryPayload>) {
      const { url } = action.payload;

      while (state.backNavigations.length > 0) {
        const lastBackNavigation = state.backNavigations[state.backNavigations.length - 1];
        if (!url.startsWith(lastBackNavigation.routeBoundary.join('/'))) {
          state.backNavigations.pop();
        } else {
          break;
        }
      }
    },
    popBackNavigation(state) {
      if (state.backNavigations.length > 0) {
        state.backNavigations.pop();
      }
    },
    resetNavigation() {
      return initialState;
    }
  }
});

function routesNotEqual(route1: string[], route2: string[]): boolean {
  if (route1.length !== route2.length) {
    return true;
  }

  for (let i = 0; i < route1.length; i += 1) {
    if (route1[i] !== route2[i]) {
      return true;
    }
  }

  return false;
}

export const {
  pushBackNavigation,
  popBackNavigationByRouteBoundary,
  popBackNavigation,
  resetNavigation
} = navigationSlice.actions;

export default navigationSlice.reducer;
