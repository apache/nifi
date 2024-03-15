const target = {
    target: 'https://localhost:8443',
    secure: false,
    logLevel: 'debug',
    changeOrigin: true,
    headers: {
        'X-ProxyPort': 4200
    },
    configure: (proxy, _options) => {
        proxy.on('error', (err, _req, _res) => {
            console.log('proxy error', err);
        });
        proxy.on('proxyReq', (proxyReq, req, _res) => {
            console.log('Sending Request to the Target:', req.method, req.url);
        });
        proxy.on('proxyRes', (proxyRes, req, _res) => {
            console.log('Received Response from the Target:', proxyRes.statusCode, req.url);
        });
    },
    bypass: function(req, res, proxyOptions) {
        if (req.url.startsWith('/nifi/')) {
            return req.url;
        }
    }
};

export default {
    '/**': target
};
