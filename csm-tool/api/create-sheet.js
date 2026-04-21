const https = require('https');

const MAKE_WEBHOOK = 'https://hook.eu2.make.com/r6mszrrgyenj983rbx6tngsjojwogr68';

function post(url, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname,
      path: u.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
      },
    }, (res) => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => resolve(data));
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const {
    company, lieferjahr, address, bonitaet,
    pvAnzahl, pvLeistung, gruenstrom, ppa,
    lieferjahrAnfang, lieferjahrEnde,
  } = req.query;

  try {
    const body = await post(MAKE_WEBHOOK, {
      company:          company    || '',
      lieferjahr:       lieferjahr || '2027',
      address:          address    || '',
      bonitaet:         (bonitaet  || 'unbekannt').toLowerCase(),
      pvAnzahl:         parseFloat(pvAnzahl   || '0'),
      pvLeistung:       parseFloat(pvLeistung || '0'),
      gruenstrom:       (gruenstrom || 'nein').toLowerCase(),
      ppa:              (ppa        || 'nein').toLowerCase(),
      lieferjahrAnfang: parseInt(lieferjahrAnfang || lieferjahr || '2027'),
      lieferjahrEnde:   parseInt(lieferjahrEnde   || lieferjahr || '2027'),
    });

    let data;
    try { data = JSON.parse(body); } catch(_) {
      return res.status(502).json({ error: 'Ungültige Antwort von Make: ' + body.slice(0, 120) });
    }
    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
