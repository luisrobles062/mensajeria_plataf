// index.js
const express = require('express');
const { pool } = require('./db');

const app = express();
app.get('/health', async (_req, res) => {
  try {
    const { rows } = await pool.query('select 1 as ok');
    res.json({ ok: rows[0].ok === 1 });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/init', async (_req, res) => {
  try {
    await pool.query(`
      create table if not exists saludos(
        id serial primary key,
        texto text not null,
        creado_en timestamptz not null default now()
      )
    `);
    const ins = await pool.query(
      'insert into saludos(texto) values($1) returning *',
      ['Hola Neon + Render']
    );
    res.json(ins.rows[0]);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Listening on ${port}`));
