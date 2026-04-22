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

    // 7. Build Gesamtlastgang — reads from Lastgänge tab, writes to Gesamtlastgang
    // Sheet starts at col B. Layout (verified from template):
    //   D7        = today's date ("Stand:")
    //   C9        = company name (Firma)
    //   C10:F10   = Marktlokations-ID per Lieferstelle (cols C-F = LS 1-4)
    //   C11:F11   = Messlokations-ID per Lieferstelle
    //   C15:F15   = Standort name per Lieferstelle
    //   C16:F16   = Straße per Lieferstelle
    //   C17:F17   = PLZ/Ort per Lieferstelle
    //   B35:F{n}  = timestamps (col B) + kW values (cols C-F, one per CSV)
    //   Col G     = SUM formula already in template — do NOT overwrite
    let gesamtError = null;
    const glTitle = sheetTitles.find(t => /gesamtlast/i.test(t));
    if (glTitle && lgTitle) {
      try {
        // Read data from Lastgänge tab (already written in step 5)
        const lgRead = await sheets.spreadsheets.values.get({
          spreadsheetId: newId,
          range: `'${lgTitle}'!A:Z`,
        });
        const lgRows = (lgRead.data.values || []);

        // Split stacked CSVs by blank separator rows into individual datasets
        const datasets = [];
        let current = [];
        for (const row of lgRows) {
          if (!row || row.every(c => !String(c).trim())) {
            if (current.length > 0) { datasets.push(current); current = []; }
          } else {
            current.push(row);
          }
        }
        if (current.length > 0) datasets.push(current);

        function isHeader(row) {
          const first = String(row[0] || '').trim();
          return first && isNaN(parseFloat(first)) && !/^\d{2}[.\-\/]/.test(first);
        }

        function detectValueCol(rows, hdr) {
          if (hdr) {
            const hi = rows[0].findIndex(h => /wert|kwh|mwh|kw\b|verbrauch|energie/i.test(String(h)));
            if (hi >= 0) return hi;
          }
          const dataRow = rows[hdr ? 1 : 0] || [];
          for (let i = dataRow.length - 1; i >= 0; i--) {
            const v = String(dataRow[i]).replace(',', '.').trim();
            if (v !== '' && !isNaN(parseFloat(v))) return i;
          }
          return Math.max(0, dataRow.length - 1);
        }

        function buildTimestamp(row) {
          const a = String(row[0] || '').trim();
          const b = String(row[1] || '').trim();
          return (b && /^\d{2}:\d{2}/.test(b)) ? `${a} ${b}` : a;
        }

        // Max 4 Lieferstellen → cols C, D, E, F (col G = template SUM)
        const numLS = Math.min(datasets.length, 4);
        const colLetter = i => String.fromCharCode(67 + i); // C=0, D=1, E=2, F=3
        const endCol = colLetter(numLS - 1);

        // Extract Lieferstellen from uploaded XLSX (by row order, matching CSVs)
        const lieferstellen = [];
        if (abnahmeRows.length > 1) {
          const hdrs = abnahmeRows[0].map(h => String(h).toLowerCase().trim());
          const ix = {
            malo:    hdrs.findIndex(h => /marktlok|^malo/.test(h)),
            melo:    hdrs.findIndex(h => /messlok|^melo/.test(h)),
            name:    hdrs.findIndex(h => /^name$|standort|bezeichnung/.test(h)),
            strasse: hdrs.findIndex(h => /stra[ßs]/i.test(h)),
            plz:     hdrs.findIndex(h => /^plz$/.test(h)),
            ort:     hdrs.findIndex(h => /^ort$|^stadt$/.test(h)),
            adresse: hdrs.findIndex(h => /^adresse?$|^anschrift$/.test(h)),
          };
          const g = (r, k) => ix[k] >= 0 ? String(r[ix[k]] || '').trim() : '';
          for (let i = 1; i < abnahmeRows.length; i++) {
            const r = abnahmeRows[i];
            if (!r.some(c => String(c).trim())) continue;
            const strasse = g(r, 'strasse');
            const plz = g(r, 'plz');
            const ort = g(r, 'ort');
            const addr = ix.adresse >= 0 ? g(r, 'adresse')
              : [strasse, [plz, ort].filter(Boolean).join(' ')].filter(Boolean).join(', ');
            lieferstellen.push({ malo: g(r,'malo'), melo: g(r,'melo'), name: g(r,'name'), addr });
          }
        }

        // Today's date for "Stand:" field
        const now2 = new Date();
        const todayStr = `${String(now2.getDate()).padStart(2,'0')}.${String(now2.getMonth()+1).padStart(2,'0')}.${now2.getFullYear()}`;

        const glBatch = [
          { range: `'${glTitle}'!D7`, values: [[todayStr]] },
          { range: `'${glTitle}'!C9`, values: [[company || '']] },
        ];

        // Lieferstellen metadata spread across cols C-F (one col per LS)
        if (numLS > 0) {
          const maloRow = [], meloRow = [], nameRow = [], strasseRow = [], plzOrtRow = [];
          for (let i = 0; i < numLS; i++) {
            const ls = lieferstellen[i] || {};
            maloRow.push(ls.malo || '');
            meloRow.push(ls.melo || '');
            nameRow.push(ls.name || '');
            const parts = (ls.addr || '').split(', ');
            strasseRow.push(parts[0] || '');
            plzOrtRow.push(parts.slice(1).join(', ') || '');
          }
          glBatch.push({ range: `'${glTitle}'!C10:${endCol}10`, values: [maloRow] });
          if (meloRow.some(v => v))    glBatch.push({ range: `'${glTitle}'!C11:${endCol}11`, values: [meloRow] });
          if (nameRow.some(v => v))    glBatch.push({ range: `'${glTitle}'!C15:${endCol}15`, values: [nameRow] });
          if (strasseRow.some(v => v)) glBatch.push({ range: `'${glTitle}'!C16:${endCol}16`, values: [strasseRow] });
          if (plzOrtRow.some(v => v))  glBatch.push({ range: `'${glTitle}'!C17:${endCol}17`, values: [plzOrtRow] });
        }

        // Data rows starting at row 35: col B = timestamp, cols C-F = kW values
        const firstDS    = datasets[0] || [];
        const firstHdr   = firstDS.length > 0 && isHeader(firstDS[0]);
        const firstStart = firstHdr ? 1 : 0;
        const numRows    = firstDS.length - firstStart;

        if (numRows > 0) {
          const dataRows = [];
          for (let r = 0; r < numRows; r++) {
            const row = [buildTimestamp(firstDS[firstStart + r] || [])];
            for (let c = 0; c < numLS; c++) {
              const ds = datasets[c];
              const hdr = ds.length > 0 && isHeader(ds[0]);
              const valCol = detectValueCol(ds, hdr);
              const dr = ds[(hdr ? 1 : 0) + r] || [];
              const raw = String(dr[valCol] || '').trim().replace(',', '.');
              row.push(raw !== '' && !isNaN(parseFloat(raw)) ? parseFloat(raw) : '');
            }
            dataRows.push(row);
          }
          glBatch.push({
            range: `'${glTitle}'!B35:${endCol}${34 + numRows}`,
            values: dataRows,
          });
        }

        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: newId,
          requestBody: { valueInputOption: 'USER_ENTERED', data: glBatch },
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
