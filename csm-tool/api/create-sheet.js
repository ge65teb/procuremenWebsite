const { google } = require('googleapis');
const XLSX = require('xlsx');

const TEMPLATE_ID       = '1NiHl8zZM_vp3PymX4HaiBe1bzPMrMaFhvPYo6xbKEFE';
const FOLDER_ID         = '1gwxFgEb3D7WIOOQrVArM50o04V9oQSaC';
const SHEET_SALES       = '0) Sales Enablement';
const SHEET_LASTGAENGE  = 'Lastgänge';
const SHEET_ABNAHME     = 'Abnahmestellen';

function getAuth() {
  if (!process.env.GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON not set.');
  return new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
    scopes: ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'],
  });
}

function toShortName(company) {
  return company
    .replace(/GmbH\s*&\s*Co\.?\s*KG/gi, '').replace(/GmbH\s*&\s*Co\./gi, '')
    .replace(/GmbH/gi, '').replace(/\bAG\b/g, '').replace(/\bSE\b/g, '')
    .replace(/\bKG\b/g, '').replace(/\bOHG\b/g, '').replace(/\bUG\b/g, '')
    .replace(/e\.K\./gi, '').replace(/\s+/g, ' ').trim();
}

function parseCSV(text) {
  const lines = text.trim().split(/\r?\n/).filter(l => l.trim());
  if (!lines.length) return [];
  const delim = (lines[0].split(';').length >= lines[0].split(',').length) ? ';' : ',';
  return lines.map(line => {
    const cells = []; let cur = ''; let inQ = false;
    for (const ch of line) {
      if (ch === '"') { inQ = !inQ; }
      else if (ch === delim && !inQ) { cells.push(cur); cur = ''; }
      else { cur += ch; }
    }
    cells.push(cur);
    return cells;
  });
}

function parseXLSX(base64) {
  const buf = Buffer.from(base64, 'base64');
  const wb  = XLSX.read(buf, { type: 'buffer' });
  const ws  = wb.Sheets[wb.SheetNames[0]];
  return XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Support both GET (legacy) and POST (with files)
  const body = req.method === 'POST' ? (req.body || {}) : req.query;
  const { company, lieferjahr, address, bonitaet, pvAnzahl, pvLeistung,
          gruenstrom, ppa, lieferjahrAnfang, lieferjahrEnde,
          csvContents, xlsxContent } = body;

  try {
    const auth   = getAuth();
    const drive  = google.drive({ version: 'v3', auth });
    const sheets = google.sheets({ version: 'v4', auth });

    // 1. Copy template
    const now     = new Date();
    const dateStr = `${now.getFullYear()}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}`;
    const fileName = `${dateStr}_${toShortName(company || 'Unbekannt')}_${lieferjahr || '2027'}`;

    const copyRes = await drive.files.copy({
      fileId: TEMPLATE_ID,
      supportsAllDrives: true,
      requestBody: { name: fileName, parents: [FOLDER_ID] },
      fields: 'id',
    });
    const newId = copyRes.data.id;
    const url   = `https://docs.google.com/spreadsheets/d/${newId}/edit`;

    // 2. Prepare Sales Enablement cells
    const addr   = (address || '').trim();
    const ci     = addr.indexOf(',');
    const street = ci >= 0 ? addr.substring(0, ci).trim() : addr;
    const city   = ci >= 0 ? addr.substring(ci + 1).trim() : '';
    const anfang = parseInt(lieferjahrAnfang || lieferjahr || '2027');
    const ende   = parseInt(lieferjahrEnde   || lieferjahr || '2027');
    const pvAnz  = parseFloat(pvAnzahl   || '0');
    const pvLei  = parseFloat(pvLeistung || '0');

    const salesData = [
      { range: `'${SHEET_SALES}'!C5`,  values: [[company || '']] },
      { range: `'${SHEET_SALES}'!C6`,  values: [[street]] },
      { range: `'${SHEET_SALES}'!C7`,  values: [[city]] },
      { range: `'${SHEET_SALES}'!C8`,  values: [[(bonitaet || '').toLowerCase()]] },
      { range: `'${SHEET_SALES}'!C21`, values: [[anfang]] },
      { range: `'${SHEET_SALES}'!C24`, values: [[(gruenstrom || 'nein').toLowerCase()]] },
      { range: `'${SHEET_SALES}'!C25`, values: [[(ppa || 'nein').toLowerCase()]] },
      { range: `'${SHEET_SALES}'!C27`, values: [[anfang]] },
      { range: `'${SHEET_SALES}'!C28`, values: [[ende]] },
    ];
    if (pvAnz > 0) salesData.push({ range: `'${SHEET_SALES}'!C13`, values: [[pvAnz]] });
    if (pvLei > 0) salesData.push({ range: `'${SHEET_SALES}'!C14`, values: [[pvLei]] });

    // 3. Prepare Lastgänge data (stack multiple CSVs)
    let lastgaengeData = null;
    if (csvContents && csvContents.length > 0) {
      const rows = [];
      csvContents.forEach((csv, i) => {
        if (i > 0) rows.push(['']); // blank separator between files
        parseCSV(csv).forEach(r => rows.push(r));
      });
      if (rows.length) {
        lastgaengeData = { range: `'${SHEET_LASTGAENGE}'!A1`, values: rows };
      }
    }

    // 4. Prepare Abnahmestellen data (from XLSX)
    let abnahmeData = null;
    if (xlsxContent) {
      const rows = parseXLSX(xlsxContent);
      if (rows.length) {
        abnahmeData = { range: `'${SHEET_ABNAHME}'!A1`, values: rows };
      }
    }

    // 5. Write all data in one batch
    const batchData = [...salesData];
    if (lastgaengeData) batchData.push(lastgaengeData);
    if (abnahmeData)    batchData.push(abnahmeData);

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: newId,
      requestBody: { valueInputOption: 'USER_ENTERED', data: batchData },
    });

    res.status(200).json({ url });
  } catch (err) {
    const detail = err.response?.data?.error || err.message;
    res.status(500).json({ error: typeof detail === 'object' ? JSON.stringify(detail) : detail });
  }
};
