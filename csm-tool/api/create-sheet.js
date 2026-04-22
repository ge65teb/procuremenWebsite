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
          gruenstrom, ppa, lieferjahrAnfang, lieferjahrEnde, energietraeger,
          csvContents, xlsxContent } = body;

  // Derive Energieart for Gesamtlastgang row 13
  const energieart = (energietraeger || '').toLowerCase() === 'gas'
    ? 'Gas'
    : (gruenstrom || '').toLowerCase() === 'ja' ? 'Ökostrom' : 'Graustrom';

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
    const meta = await sheets.spreadsheets.get({ spreadsheetId: newId, fields: 'sheets.properties' });
    const sheetTitles = meta.data.sheets.map(s => s.properties.title);
    const sheetIdMap  = Object.fromEntries(meta.data.sheets.map(s => [s.properties.title, s.properties.sheetId]));

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

        // Returns true if the row is a text header (not a date/number row)
        function isHeader(row) {
          const first = String(row[0] || '').trim();
          return first && isNaN(parseFloat(first.replace(',','.'))) && !/^\d{2}[.\-\/]/.test(first);
        }

        // Returns true if a cell value is a timestamp-like string (date or time), not a measurement
        function isTimestampLike(v) {
          const s = String(v || '').trim();
          return /^\d{2}:\d{2}/.test(s)               // HH:MM time
              || /^\d{2}[.\-\/]\d{2}[.\-\/]/.test(s)  // DD.MM.YYYY or similar date
              || /^\d{4}-\d{2}-\d{2}/.test(s);         // ISO date
        }

        // For a dataset, returns the indices of all value (measurement) columns.
        // Col 0 is always the primary timestamp; additional date/time cols are skipped.
        function getValueCols(ds, hdr) {
          const dataRow = ds[hdr ? 1 : 0] || [];
          const valueCols = [];
          for (let i = 1; i < dataRow.length; i++) {
            const v = String(dataRow[i] || '').trim();
            if (!v) continue;
            if (isTimestampLike(v)) continue; // skip end-timestamp or time col
            const num = v.replace(',', '.');
            if (!isNaN(parseFloat(num))) valueCols.push(i);
          }
          // If header row exists, also add cols whose header indicates a value
          if (hdr && valueCols.length === 0) {
            ds[0].forEach((h, i) => {
              if (i > 0 && /wert|kwh|mwh|kw\b|verbrauch|energie|leistung/i.test(String(h)))
                valueCols.push(i);
            });
          }
          return valueCols;
        }

        // Build the timestamp string: combine col 0 + col 1 if col 1 is a time (HH:MM)
        function buildTimestamp(row, valueCols) {
          const a = String(row[0] || '').trim();
          const b = String(row[1] || '').trim();
          // Append col 1 only if it's a time and NOT a value column
          if (b && /^\d{2}:\d{2}/.test(b) && !valueCols.includes(1)) return `${a} ${b}`;
          return a;
        }

        // Analyse each dataset → { start (data row index), valueCols }
        const dsInfo = datasets.map(ds => {
          const hdr = ds.length > 0 && isHeader(ds[0]);
          return { hdr, start: hdr ? 1 : 0, valueCols: getValueCols(ds, hdr) };
        });

        // Flatten: collect all (datasetIndex, colIndex) pairs in order
        // This becomes the global list of value columns → mapped 1:1 to Lieferstellen
        const allValCols = [];
        dsInfo.forEach((info, di) => {
          info.valueCols.forEach(ci => allValCols.push({ di, ci }));
        });

        // No cap: support all value columns found in the data
        const numCols  = allValCols.length;
        // Converts 0-based value-column index to spreadsheet column letter(s).
        // i=0 → 'C', i=1 → 'D', …, i=23 → 'Z', i=24 → 'AA', i=25 → 'AB', …
        const colLetter = i => {
          let n = i + 3; // C is the 3rd column (A=1, B=2, C=3)
          let s = '';
          while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
          return s;
        };
        const endCol   = numCols > 0 ? colLetter(numCols - 1) : 'C';

        // If >4 columns: insert extra columns before the sum column (G = sheet col-index 6).
        // Inserting before G causes Sheets to auto-extend the sum formula from sum(C:F) → sum(C:H) etc.
        const numExtraCols = Math.max(0, numCols - 4);
        if (numExtraCols > 0 && sheetIdMap[glTitle] !== undefined) {
          await sheets.spreadsheets.batchUpdate({
            spreadsheetId: newId,
            requestBody: {
              requests: [{
                insertDimension: {
                  range: {
                    sheetId:    sheetIdMap[glTitle],
                    dimension:  'COLUMNS',
                    startIndex: 6,                     // before col G (0-indexed from A)
                    endIndex:   6 + numExtraCols,
                  },
                  inheritFromBefore: true,             // copy formatting from col F
                },
              }],
            },
          });
          // Write column headers for the new columns (rows 29 + 34)
          const extraHdrs = Array(numExtraCols).fill('Wirkleistung (Entnahme)');
          const extraWert = Array(numExtraCols).fill('Wert (kW)');
          const extraSum  = [];
          for (let i = 4; i < numCols; i++) {
            extraSum.push(`=SUM(${colLetter(i)}35:${colLetter(i)}35266)/4`);
          }
          const extraStartCol = colLetter(4); // G after insert
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: newId,
            requestBody: {
              valueInputOption: 'USER_ENTERED',
              data: [
                { range: `'${glTitle}'!${extraStartCol}29:${endCol}29`, values: [extraHdrs] },
                { range: `'${glTitle}'!${extraStartCol}32:${endCol}32`, values: [extraSum]  },
                { range: `'${glTitle}'!${extraStartCol}34:${endCol}34`, values: [extraWert] },
              ],
            },
          });
        }

        // Extract Lieferstellen from XLSX — i-th data row → i-th value column
        const lieferstellen = [];
        const xlsxDebugHdrs = abnahmeRows.length > 0
          ? abnahmeRows[0].map(h => String(h)).join(' | ') : '(keine XLSX-Daten)';
        // ── Lieferstellen matching ────────────────────────────────────────────
        // Step 1: extract the column header name for each value column from Lastgänge.
        //   This is the Standort label used for matching (e.g. "Werk Berlin (kW)").
        const valColNames = allValCols.map(({ di, ci }) => {
          const ds = datasets[di];
          const info = dsInfo[di];
          return info.hdr && ds[0] ? String(ds[0][ci] || '').trim() : '';
        });

        // Step 2: normalise a string for fuzzy comparison
        function normName(s) {
          return String(s || '').toLowerCase()
            .replace(/\[.*?\]|\(.*?\)/g, '')          // strip [unit] and (note)
            .replace(/\bkwh?\b|\bmwh?\b/gi, '')        // strip unit words
            .replace(/[^a-z0-9äöüß]+/gi, ' ')
            .replace(/\s+/g, ' ').trim();
        }

        // Step 3: score similarity (0–100) between two name strings
        function nameScore(a, b) {
          const na = normName(a); const nb = normName(b);
          if (!na || !nb) return 0;
          if (na === nb) return 100;
          if (na.includes(nb) || nb.includes(na)) return 80;
          const wa = new Set(na.split(' ').filter(w => w.length > 2));
          const wb = new Set(nb.split(' ').filter(w => w.length > 2));
          const hits = [...wa].filter(w => wb.has(w)).length;
          const total = new Set([...wa, ...wb]).size;
          return total > 0 ? Math.round((hits / total) * 60) : 0;
        }

        // Step 4: parse Abnahmestellen XLSX into structured rows
        const abDataRows = [];  // array of { malo, melo, messstelle, energieart, name, strasse, plzOrt }
        if (abnahmeRows.length > 1) {
          const hdrs = abnahmeRows[0].map(h => String(h).toLowerCase().trim());
          const ix = {
            malo:       hdrs.findIndex(h => /marktlok|^malo/.test(h)),
            melo:       hdrs.findIndex(h => /messlok|^melo/.test(h)),
            messstelle: hdrs.findIndex(h => /messstelle|zählerart|zählertyp|^rlm$|^slp$/.test(h)),
            energieart: hdrs.findIndex(h => /energieart|stromtyp|^öko|^grün/.test(h)),
            name:       hdrs.findIndex(h => /standort|bezeichnung|^name$|lieferst/.test(h)),
            strasse:    hdrs.findIndex(h => /stra[ßs]/i.test(h)),
            plz:        hdrs.findIndex(h => /^plz$/.test(h)),
            ort:        hdrs.findIndex(h => /^ort$|^stadt$/.test(h)),
            adresse:    hdrs.findIndex(h => /^adresse?$|^anschrift$/.test(h)),
          };
          const g = (r, k) => ix[k] >= 0 ? String(r[ix[k]] || '').trim() : '';
          for (let i = 1; i < abnahmeRows.length; i++) {
            const r = abnahmeRows[i];
            if (!r || !r.some(c => String(c).trim())) continue;
            // Only include RLM metering points
            const messstelle = g(r, 'messstelle');
            if (messstelle && !/rlm/i.test(messstelle)) continue;
            const strasse = g(r, 'strasse');
            const plz = g(r, 'plz'); const ort = g(r, 'ort');
            const plzOrt = [plz, ort].filter(Boolean).join(' ');
            const addrFull = ix.adresse >= 0 ? g(r, 'adresse') : strasse;
            abDataRows.push({
              malo: g(r,'malo'), melo: g(r,'melo'),
              messstelle: g(r,'messstelle'), energieart: g(r,'energieart'),
              name: g(r,'name'), strasse: addrFull, plzOrt,
            });
          }
        }

        // Step 5: for each value column, find best-matching Abnahmestellen row by name
        const matchDebug = [];
        const matchedLS = valColNames.map((colName, i) => {
          let best = null; let bestScore = -1;
          for (const ab of abDataRows) {
            const score = nameScore(colName, ab.name);
            if (score > bestScore) { bestScore = score; best = ab; }
          }
          matchDebug.push({ col: i, colName, matched: best?.name || '–', score: bestScore });
          // fall back to index order if score is 0 (no names available)
          return bestScore > 0 ? best : (abDataRows[i] || null);
        });

        // ── Today's date ──────────────────────────────────────────────────────
        const now2 = new Date();
        const todayStr = `${String(now2.getDate()).padStart(2,'0')}.${String(now2.getMonth()+1).padStart(2,'0')}.${now2.getFullYear()}`;

        const glBatch = [
          { range: `'${glTitle}'!D7`, values: [[todayStr]] },
          { range: `'${glTitle}'!C9`, values: [[company || '']] },
        ];

        // ── Metadata rows 10-17 (one spreadsheet column per Lieferstelle) ─────
        if (numCols > 0) {
          const r10 = [], r11 = [], r12 = [], r13 = [], r15 = [], r16 = [], r17 = [];
          for (let i = 0; i < numCols; i++) {
            const ls = matchedLS[i] || {};
            r10.push(ls.malo       || '');
            r11.push(ls.melo       || '');
            r12.push(ls.messstelle || 'RLM');
            r13.push(energieart);
            r15.push(valColNames[i] || ls.name || '');  // Standort from CSV header
            r16.push(ls.strasse    || '');
            r17.push(ls.plzOrt     || '');
          }
          glBatch.push({ range: `'${glTitle}'!C10:${endCol}10`, values: [r10] });
          if (r11.some(v => v)) glBatch.push({ range: `'${glTitle}'!C11:${endCol}11`, values: [r11] });
          if (r12.some(v => v)) glBatch.push({ range: `'${glTitle}'!C12:${endCol}12`, values: [r12] });
          if (r13.some(v => v)) glBatch.push({ range: `'${glTitle}'!C13:${endCol}13`, values: [r13] });
          if (r15.some(v => v)) glBatch.push({ range: `'${glTitle}'!C15:${endCol}15`, values: [r15] });
          if (r16.some(v => v)) glBatch.push({ range: `'${glTitle}'!C16:${endCol}16`, values: [r16] });
          if (r17.some(v => v)) glBatch.push({ range: `'${glTitle}'!C17:${endCol}17`, values: [r17] });
        }

        // Data rows: col B = timestamp, cols C+ = one kW value per Lieferstelle
        const firstDS    = datasets[0] || [];
        const firstInfo  = dsInfo[0] || { start: 0, valueCols: [] };
        const numRows    = firstDS.length - firstInfo.start;

        if (numRows > 0 && numCols > 0) {
          const dataRows = [];
          for (let r = 0; r < numRows; r++) {
            const firstRow = firstDS[firstInfo.start + r] || [];
            const ts = buildTimestamp(firstRow, firstInfo.valueCols);
            const outRow = [ts];
            for (let c = 0; c < numCols; c++) {
              const { di, ci } = allValCols[c];
              const info = dsInfo[di];
              const dr = (datasets[di] || [])[info.start + r] || [];
              const raw = String(dr[ci] || '').trim().replace(',', '.');
              outRow.push(raw !== '' && !isNaN(parseFloat(raw)) ? parseFloat(raw) : '');
            }
            dataRows.push(outRow);
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

    res.status(200).json({ url, sheetTitles, abnahmeError, gesamtError,
      _debug: { xlsxRows: abnahmeRows.length, xlsxHdrs: abnahmeRows[0] || [] } });
  } catch (err) {
    const detail = err.response?.data?.error || err.message;
    res.status(500).json({ error: typeof detail === 'object' ? JSON.stringify(detail) : detail });
  }
};
