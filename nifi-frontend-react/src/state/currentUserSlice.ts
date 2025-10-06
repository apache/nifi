import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface CurrentUser {
  identity?: string;
  anonymous?: boolean;
  [key: string]: unknown;
}

export interface CurrentUserState {
  status: 'idle' | 'loading' | 'success' | 'error';
  user?: CurrentUser;
  error?: string;
}

const initialState: CurrentUserState = {
  status: 'idle'
};

const currentUserSlice = createSlice({
  name: 'currentUser',
  initialState,
  reducers: {
    setCurrentUserStatus(state, action: PayloadAction<CurrentUserState['status']>) {
      state.status = action.payload;
      if (action.payload !== 'error') {
        state.error = undefined;
      }
    },
    setCurrentUser(state, action: PayloadAction<CurrentUser | undefined>) {
      state.user = action.payload;
    },
    setCurrentUserError(state, action: PayloadAction<string | undefined>) {
      state.error = action.payload ?? 'Unknown error';
      state.status = 'error';
    },
    resetCurrentUser() {
      return initialState;
    }
  }
});

export const { setCurrentUserStatus, setCurrentUser, setCurrentUserError, resetCurrentUser } =
  currentUserSlice.actions;

export default currentUserSlice.reducer;
