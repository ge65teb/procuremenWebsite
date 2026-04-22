const { google } = require('googleapis');

const TEMPLATE_ID = '1NiHl8zZM_vp3PymX4HaiBe1bzPMrMaFhvPYo6xbKEFE';
const FOLDER_ID   = '1gwxFgEb3D7WIOOQrVArM50o04V9oQSaC';
const SHEET_NAME  = '0) Sales Enablement';

function getAuth() {
  if (!process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON environment variable is not set.');
  }
  const key = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
  return new google.auth.GoogleAuth({
    credentials: key,
    scopes: [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/spreadsheets',
    ],
  });
}

function toShortName(company) {
  return company
    .replace(/GmbH\s*&\s*Co\.?\s*KG/gi, '')
    .replace(/GmbH\s*&\s*Co\./gi, '')
    .replace(/GmbH/gi, '').replace(/\bAG\b/g, '').replace(/\bSE\b/g, '')
    .replace(/\bKG\b/g, '').replace(/\bOHG\b/g, '').replace(/\bUG\b/g, '')
    .replace(/e\.K\./gi, '').replace(/\s+/g, ' ').trim();
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const {
    company, lieferjahr, address, bonitaet,
    pvAnzahl, pvLeistung, gruenstrom, ppa,
    lieferjahrAnfang, lieferjahrEnde,
  } = req.query;

  try {
    const auth   = getAuth();
    const drive  = google.drive({ version: 'v3', auth });
    const sheets = google.sheets({ version: 'v4', auth });

    // 1. Build filename & copy template
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

    // 2. Prepare cell values
    const addr   = (address || '').trim();
    const ci     = addr.indexOf(',');
    const street = ci >= 0 ? addr.substring(0, ci).trim() : addr;
    const city   = ci >= 0 ? addr.substring(ci + 1).trim() : '';
    const anfang = parseInt(lieferjahrAnfang || lieferjahr || '2027');
    const ende   = parseInt(lieferjahrEnde   || lieferjahr || '2027');
    const pvAnz  = parseFloat(pvAnzahl   || '0');
    const pvLei  = parseFloat(pvLeistung || '0');

    const data = [
      { range: `'${SHEET_NAME}'!C5`,  values: [[company || '']] },
      { range: `'${SHEET_NAME}'!C6`,  values: [[street]] },
      { range: `'${SHEET_NAME}'!C7`,  values: [[city]] },
      { range: `'${SHEET_NAME}'!C8`,  values: [[(bonitaet || '').toLowerCase()]] },
      { range: `'${SHEET_NAME}'!C21`, values: [[anfang]] },
      { range: `'${SHEET_NAME}'!C24`, values: [[(gruenstrom || 'nein').toLowerCase()]] },
      { range: `'${SHEET_NAME}'!C25`, values: [[(ppa || 'nein').toLowerCase()]] },
      { range: `'${SHEET_NAME}'!C27`, values: [[anfang]] },
      { range: `'${SHEET_NAME}'!C28`, values: [[ende]] },
    ];
    if (pvAnz > 0) data.push({ range: `'${SHEET_NAME}'!C13`, values: [[pvAnz]] });
    if (pvLei > 0) data.push({ range: `'${SHEET_NAME}'!C14`, values: [[pvLei]] });

    // 3. Write cells
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: newId,
      requestBody: { valueInputOption: 'USER_ENTERED', data },
    });

    res.status(200).json({ url });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
