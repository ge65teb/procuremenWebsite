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

    // 4. Fetch actual sheet names from the copied file
    const meta = await sheets.spreadsheets.get({ spreadsheetId: newId, fields: 'sheets.properties.title' });
    const sheetTitles = meta.data.sheets.map(s => s.properties.title);

    function findSheet(keyword) {
      return sheetTitles.find(t => t.toLowerCase().includes(keyword.toLowerCase())) || null;
    }

    // 5. Write Sales + Lastgänge in one batch
    const batchData = [...salesData];

    // Prefer exact "Lastgänge" tab; avoid matching "Gesamtlastgang" etc.
    const lgTitle = sheetTitles.find(t => /^lastg[äa]/i.test(t.trim()))
                 || sheetTitles.find(t => t.toLowerCase() === 'lastgänge')
                 || findSheet('lastgänge')
                 || findSheet('lastgaenge');
    if (lastgaengeData && lgTitle) {
      batchData.push({ ...lastgaengeData, range: `'${lgTitle}'!A1` });
    } else if (lastgaengeData) {
      batchData.push(lastgaengeData);
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: newId,
      requestBody: { valueInputOption: 'USER_ENTERED', data: batchData },
    });

    // 6. Write Abnahmestellen separately (non-fatal)
    let abnahmeRows = [];
    let abnahmeError = null;
    if (xlsxContent) {
      abnahmeRows = parseXLSX(xlsxContent);
      if (abnahmeRows.length) {
        const abTitle = findSheet('abnahme') || findSheet('lieferst') || null;
        if (abTitle) {
          try {
            await sheets.spreadsheets.values.batchUpdate({
              spreadsheetId: newId,
              requestBody: {
                valueInputOption: 'USER_ENTERED',
                data: [{ range: `'${abTitle}'!A1`, values: abnahmeRows }],
              },
            });
          } catch (e) {
            abnahmeError = e.message;
          }
        } else {
          abnahmeError = `Tab not found. Available tabs: ${sheetTitles.join(', ')}`;
        }
      }
    }

    // 7. Build Gesamtlastgang: col A = auto-sum (template formula), col B = timestamp,
    //    cols C+ = one consumption column per CSV/Lieferstelle
    let gesamtError = null;
    const glTitle = sheetTitles.find(t => /gesamtlast/i.test(t));
    if (glTitle && csvContents && csvContents.length > 0) {
      try {
        // Parse each CSV individually (not stacked)
        const datasets = csvContents.map(csv =>
          parseCSV(csv).filter(r => r.some(c => String(c).trim()))
        );

        // Detect if first row is a text header (not a date/number)
        function isHeader(row) {
          const first = String(row[0] || '').trim();
          return first && isNaN(parseFloat(first)) && !/^\d{2}[.\-\/]/.test(first);
        }

        // From a CSV dataset, detect which column index holds the value
        function detectValueCol(rows, headerRow) {
          if (headerRow) {
            const hi = rows[0].findIndex(h =>
              /wert|kwh|mwh|kw\b|verbrauch|energie/i.test(String(h))
            );
            if (hi >= 0) return hi;
          }
          // Fall back: last column that is numeric in first data row
          const dataRow = rows[headerRow ? 1 : 0] || [];
          for (let i = dataRow.length - 1; i >= 0; i--) {
            const v = String(dataRow[i]).replace(',', '.').trim();
            if (v !== '' && !isNaN(parseFloat(v))) return i;
          }
          return dataRow.length - 1;
        }

        // Build timestamp list from first dataset
        const firstDS   = datasets[0] || [];
        const firstHdr  = firstDS.length > 0 && isHeader(firstDS[0]);
        const firstStart = firstHdr ? 1 : 0;

        // Detect timestamp cols (some CSVs have date + time in separate cols)
        function buildTimestamp(row) {
          const a = String(row[0] || '').trim();
          const b = String(row[1] || '').trim();
          // If col 1 looks like a time (HH:MM) append it
          if (b && /^\d{2}:\d{2}/.test(b)) return `${a} ${b}`;
          return a;
        }

        // Extract Lieferstellen info from XLSX (matched by row index to CSVs)
        const lieferstellen = [];
        if (abnahmeRows.length > 1) {
          const hdrs = abnahmeRows[0].map(h => String(h).toLowerCase().trim());
          const maloIdx    = hdrs.findIndex(h => /marktlok|malo\b/.test(h));
          const meloIdx    = hdrs.findIndex(h => /messlok|melo\b/.test(h));
          const strasseIdx = hdrs.findIndex(h => /stra[ßs]/i.test(h));
          const plzIdx     = hdrs.findIndex(h => /^plz$|postleitz/.test(h));
          const ortIdx     = hdrs.findIndex(h => /^ort$|^stadt$|^gemeinde$/.test(h));
          // also try a single "Adresse" column
          const adresseIdx = hdrs.findIndex(h => /^adresse?$|^anschrift$/.test(h));

          for (let i = 1; i < abnahmeRows.length; i++) {
            const r = abnahmeRows[i];
            if (!r.some(c => String(c).trim())) continue;
            const malo = maloIdx >= 0 ? String(r[maloIdx] || '').trim() : '';
            const melo = meloIdx >= 0 ? String(r[meloIdx] || '').trim() : '';
            let addr = '';
            if (adresseIdx >= 0) {
              addr = String(r[adresseIdx] || '').trim();
            } else {
              const strasse = strasseIdx >= 0 ? String(r[strasseIdx] || '').trim() : '';
              const plz     = plzIdx  >= 0 ? String(r[plzIdx]  || '').trim() : '';
              const ort     = ortIdx  >= 0 ? String(r[ortIdx]  || '').trim() : '';
              addr = [strasse, [plz, ort].filter(Boolean).join(' ')].filter(Boolean).join(', ');
            }
            lieferstellen.push({ malo, melo, addr });
          }
        }

        // Build output: row 1 = MaLo header, row 2 = Address header, rows 3+ = data
        const maloRow = ['', 'Zeitstempel'];
        const addrRow = ['', ''];
        datasets.forEach((_, i) => {
          const ls = lieferstellen[i];
          maloRow.push(ls?.malo || `Lieferstelle ${i + 1}`);
          addrRow.push(ls?.addr || '');
        });

        const dataRows = [];
        const numRows = firstDS.length - firstStart;
        for (let r = 0; r < numRows; r++) {
          const out = ['', buildTimestamp(firstDS[firstStart + r] || [])];
          datasets.forEach(ds => {
            const hdr = ds.length > 0 && isHeader(ds[0]);
            const start = hdr ? 1 : 0;
            const valCol = detectValueCol(ds, hdr);
            const row = ds[start + r] || [];
            // German decimal comma → dot for Sheets USER_ENTERED
            const raw = String(row[valCol] || '').trim().replace(',', '.');
            out.push(raw !== '' && !isNaN(parseFloat(raw)) ? parseFloat(raw) : '');
          });
          dataRows.push(out);
        }

        const glValues = [maloRow, addrRow, ...dataRows];
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: newId,
          requestBody: {
            valueInputOption: 'USER_ENTERED',
            data: [{ range: `'${glTitle}'!A1`, values: glValues }],
          },
        });
      } catch (e) {
        gesamtError = e.message;
      }
    }

    res.status(200).json({ url, sheetTitles, abnahmeError, gesamtError });
  } catch (err) {
    const detail = err.response?.data?.error || err.message;
    res.status(500).json({ error: typeof detail === 'object' ? JSON.stringify(detail) : detail });
  }
};
