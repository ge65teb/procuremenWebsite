const https = require('https');

const SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbzEVf4FuOIH8yZsGPwz-76tlazapfAFSRTVcIwI47_M3wBa78UiAQIgtt9rWZxn4ZrmCg/exec';

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'node' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return httpsGet(res.headers.location).then(resolve).catch(reject);
      }
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => resolve(body));
    }).on('error', reject);
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Forward all query params to Apps Script
  const qs = new URLSearchParams(req.query).toString();

  try {
    const body = await httpsGet(SCRIPT_URL + '?' + qs);
    let data;
    try { data = JSON.parse(body); } catch(_) { return res.status(502).json({ error: 'Ungültige Antwort vom Apps Script: ' + body.slice(0, 120) }); }
    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
