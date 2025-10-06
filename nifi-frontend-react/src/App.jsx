
import React from 'react';
import { Provider } from 'react-redux';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import store from './state/store';
import FlowDesigner from './pages/FlowDesigner';
import AuthenticationGuard from './components/guards/AuthenticationGuard';
import LoginConfigurationGuard from './components/guards/LoginConfigurationGuard';
import Login from './pages/Login';

function App() {
  return (
    <Provider store={store}>
      <BrowserRouter>
        <Routes>
          <Route
            path="/login"
            element={
              <LoginConfigurationGuard
                check={(loginConfiguration) => loginConfiguration?.loginSupported === true}
                errorFallback={(message) => <div>{message}</div>}
              >
                <Login />
              </LoginConfigurationGuard>
            }
          />
          <Route
            path="/flow-designer/*"
            element={
              <AuthenticationGuard>
                <FlowDesigner />
              </AuthenticationGuard>
            }
          />
          {/* Add more routes as needed */}
        </Routes>
      </BrowserRouter>
    </Provider>
  );
}

export default App;
