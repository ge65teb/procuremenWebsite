const SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbzEVf4FuOIH8yZsGPwz-76tlazapfAFSRTVcIwI47_M3wBa78UiAQIgtt9rWZxn4ZrmCg/exec';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { company, lieferjahr } = req.query;
  const qs = new URLSearchParams({
    company:    company    || 'Unbekannt',
    lieferjahr: lieferjahr || '2027',
  });

  try {
    const response = await fetch(`${SCRIPT_URL}?${qs}`, { redirect: 'follow' });
    const data = await response.json();
    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
