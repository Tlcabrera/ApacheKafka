-- Datos sintéticos para los laboratorios 09, 10 y 14. Ningún NIT corresponde a un contribuyente real.
CREATE TABLE IF NOT EXISTS declaraciones (
  id_declaracion   SERIAL PRIMARY KEY,
  nit              TEXT NOT NULL,
  formulario       TEXT NOT NULL,
  periodo          TEXT NOT NULL,
  valor_pagado     NUMERIC(16,2) NOT NULL,
  estado           TEXT NOT NULL DEFAULT 'PRESENTADA',
  actualizado_en   TIMESTAMPTZ NOT NULL DEFAULT now()   -- columna de control para el conector JDBC incremental
);
CREATE INDEX IF NOT EXISTS ix_decl_actualizado ON declaraciones(actualizado_en);

INSERT INTO declaraciones (nit, formulario, periodo, valor_pagado)
SELECT
  CASE WHEN random() < 0.4 THEN '9' ELSE '8' END || lpad((random()*99999999)::bigint::text, 8, '0'),
  (ARRAY['110','300','350','490'])[1 + floor(random()*4)],
  '2026-' || lpad((1 + floor(random()*9))::text, 2, '0'),
  round((random()*50000000)::numeric, 2)
FROM generate_series(1, 500);

-- Tabla destino de los laboratorios 06/07 (por si se usa el mismo PostgreSQL)
CREATE TABLE IF NOT EXISTS facturas (
  id_evento TEXT PRIMARY KEY, nit_emisor TEXT, cufe TEXT, valor_total NUMERIC, estado TEXT,
  particion INT, "offset" BIGINT, procesado_en TIMESTAMPTZ DEFAULT now());
