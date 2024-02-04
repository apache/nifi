const target = {
    target: 'https://localhost:8443',
    secure: false,
    logLevel: 'debug',
    changeOrigin: true,
    headers: {
        'X-ProxyPort': 4200
    }
};

export default {
    '/nifi-api/*': target,
    '/nifi-docs/*': target,
    '/nifi-content-viewer/*': target,
    // the following entry is needed because the content viewer (and other UIs) load resources from existing nifi ui
    '/nifi/*': target
};
