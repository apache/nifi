import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface About {
  title: string;
  version: string;
  uri: string;
  contentViewerUrl: string;
  timezone: string;
  buildTag?: string;
  buildRevision?: string;
  buildBranch?: string;
  buildTimestamp?: string;
  [key: string]: unknown;
}

export interface AboutState {
  status: 'idle' | 'loading' | 'success' | 'error';
  about?: About;
  error?: string;
}

const initialState: AboutState = {
  status: 'idle'
};

const aboutSlice = createSlice({
  name: 'about',
  initialState,
  reducers: {
    setAboutStatus(state, action: PayloadAction<AboutState['status']>) {
      state.status = action.payload;
      if (action.payload !== 'error') {
        state.error = undefined;
      }
    },
    setAbout(state, action: PayloadAction<About | undefined>) {
      state.about = action.payload;
    },
    setAboutError(state, action: PayloadAction<string | undefined>) {
      state.error = action.payload ?? 'Unknown error';
      state.status = 'error';
    },
    resetAbout() {
      return initialState;
    }
  }
});

export const { setAboutStatus, setAbout, setAboutError, resetAbout } = aboutSlice.actions;

export default aboutSlice.reducer;
