import os
import logging
from datetime import datetime
from contextlib import contextmanager
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import pandas as pd
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file

app = Flask(__name__)
app.secret_key = 'secreto'
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

# =========================
#  Conexión a Postgres/Neon
# =========================

def normalize_db_url(raw_url: str) -> str:
    if not raw_url:
        return raw_url
    parsed = urlparse(raw_url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q.pop("channel_binding", None)
    if "sslmode" not in q:
        q["sslmode"] = "require"
    if "application_name" not in q:
        q["application_name"] = "mensajeria_plataf"
    new_query = urlencode(q)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

DATABASE_URL = normalize_db_url(os.getenv("DATABASE_URL", ""))
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no está definida.")

POOL_MIN = int(os.getenv("PG_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("PG_POOL_MAX", "5"))

pool = ThreadedConnectionPool(minconn=POOL_MIN, maxconn=POOL_MAX, dsn=DATABASE_URL)

@contextmanager
def get_conn():
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    finally:
        pool.putconn(conn)

def db_exec(sql, params=()):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)

def db_fetchone_dict(sql, params=()):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()

def db_fetchall_dict(sql, params=()):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

def read_sql_df(sql, params=None):
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=params)

# =========================
#   Esquema
# =========================

def ensure_schema():
    db_exec("""
        CREATE TABLE IF NOT EXISTS zonas (
            nombre TEXT PRIMARY KEY,
            tarifa NUMERIC NOT NULL
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS mensajeros (
            nombre TEXT PRIMARY KEY,
            zona TEXT REFERENCES zonas(nombre)
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS guias (
            remitente TEXT,
            numero_guia TEXT PRIMARY KEY,
            destinatario TEXT,
            direccion TEXT,
            ciudad TEXT
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS despachos (
            numero_guia TEXT PRIMARY KEY,
            mensajero TEXT,
            zona TEXT,
            fecha TIMESTAMPTZ NOT NULL
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS recepciones (
            numero_guia TEXT PRIMARY KEY,
            tipo TEXT,
            motivo TEXT,
            fecha TIMESTAMPTZ NOT NULL
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS recogidas (
            id SERIAL PRIMARY KEY,
            numero_guia TEXT,
            fecha TIMESTAMPTZ NOT NULL,
            observaciones TEXT
        );
    """)

# =========================
#   Modelos en memoria
# =========================

class Zona:
    def __init__(self, nombre, tarifa):
        self.nombre = nombre
        self.tarifa = float(tarifa) if tarifa is not None else 0.0

class Mensajero:
    def __init__(self, nombre, zona):
        self.nombre = nombre
        self.zona = zona

zonas = []
mensajeros = []
guias = pd.DataFrame(columns=['remitente', 'numero_guia', 'destinatario', 'direccion', 'ciudad'])
despachos = []
recepciones = []
recogidas = []

# =========================
#   Cargar datos desde DB
# =========================

def cargar_datos_desde_db():
    global zonas, mensajeros, guias, despachos, recepciones, recogidas
    zrows = db_fetchall_dict("SELECT nombre, tarifa FROM zonas;")
    zonas = [Zona(r["nombre"], r["tarifa"]) for r in zrows]

    mrows = db_fetchall_dict("SELECT nombre, zona FROM mensajeros;")
    zonas_map = {z.nombre: z for z in zonas}
    mensajeros = [Mensajero(r["nombre"], zonas_map.get(r["zona"])) for r in mrows]

    guias = read_sql_df("SELECT remitente, numero_guia, destinatario, direccion, ciudad FROM guias;")

    despachos = db_fetchall_dict("SELECT numero_guia, mensajero, zona, fecha FROM despachos ORDER BY fecha DESC;")
    recepciones = db_fetchall_dict("SELECT numero_guia, tipo, motivo, fecha FROM recepciones ORDER BY fecha DESC;")
    recogidas = db_fetchall_dict("SELECT numero_guia, fecha, observaciones FROM recogidas ORDER BY fecha DESC;")

    globals().update({"zonas": zonas, "mensajeros": mensajeros, "guias": guias,
                      "despachos": despachos, "recepciones": recepciones, "recogidas": recogidas})

logging.basicConfig(level=logging.INFO)
ensure_schema()
cargar_datos_desde_db()

# =========================
#   Rutas básicas (index, cargar base, zonas, mensajeros, despachos, recepciones, liquidacion, recogidas)
# =========================

# ... AQUÍ VAN TODAS TUS RUTAS EXISTENTES (ya las tienes) ...
# Las mantendremos tal cual tu app.py actual

# =========================
#   Consultas avanzadas y exportaciones a Excel
# =========================

@app.route("/consultar_pendientes", methods=["GET", "POST"])
def consultar_pendientes():
    resultados = []
    if request.method == 'POST':
        mensajero_nombre = request.form.get('mensajero')
        fecha_inicio = request.form.get('fecha_inicio')
        fecha_fin = request.form.get('fecha_fin')
        query = """
            SELECT g.remitente, g.numero_guia, g.destinatario, g.direccion, g.ciudad,
                   d.mensajero, d.fecha as fecha_despacho
            FROM guias g
            JOIN despachos d ON g.numero_guia = d.numero_guia
            LEFT JOIN recepciones r ON g.numero_guia = r.numero_guia
            WHERE r.numero_guia IS NULL AND d.mensajero = %s AND DATE(d.fecha) BETWEEN %s AND %s
        """
        resultados = db_fetchall_dict(query, (mensajero_nombre, fecha_inicio, fecha_fin))
    return render_template('consultar_pendientes.html', resultados=resultados, mensajeros=mensajeros)

@app.route("/exportar_pendientes", methods=["POST"])
def exportar_pendientes():
    mensajero_nombre = request.form.get('mensajero')
    fecha_inicio = request.form.get('fecha_inicio')
    fecha_fin = request.form.get('fecha_fin')
    query = """
        SELECT g.remitente, g.numero_guia, g.destinatario, g.direccion, g.ciudad,
               d.mensajero, d.fecha as fecha_despacho
        FROM guias g
        JOIN despachos d ON g.numero_guia = d.numero_guia
        LEFT JOIN recepciones r ON g.numero_guia = r.numero_guia
        WHERE r.numero_guia IS NULL AND d.mensajero = %s AND DATE(d.fecha) BETWEEN %s AND %s
    """
    df = read_sql_df(query, (mensajero_nombre, fecha_inicio, fecha_fin))
    filepath = os.path.join(DATA_DIR, f'pendientes_{mensajero_nombre}.xlsx')
    df.to_excel(filepath, index=False)
    return send_file(filepath, as_attachment=True)

# =========================
#   Estado avanzado de guías
# =========================

@app.route("/consultar_estado_avanzado", methods=["GET", "POST"])
def consultar_estado_avanzado():
    resultados = []
    if request.method == 'POST':
        numeros = request.form.get('numeros', '')
        lista_numeros = [n.strip() for n in numeros.replace('\n', ',').split(',') if n.strip()]
        for numero in lista_numeros:
            guia = db_fetchone_dict("SELECT * FROM guias WHERE numero_guia=%s", (numero,))
            despacho = db_fetchone_dict("SELECT * FROM despachos WHERE numero_guia=%s", (numero,))
            recepcion = db_fetchone_dict("SELECT * FROM recepciones WHERE numero_guia=%s", (numero,))
            resultados.append({
                'numero_guia': numero,
                'remitente': guia['remitente'] if guia else '',
                'destinatario': guia['destinatario'] if guia else '',
                'direccion': guia['direccion'] if guia else '',
                'ciudad': guia['ciudad'] if guia else '',
                'mensajero': despacho['mensajero'] if despacho else '',
                'fecha_despacho': despacho['fecha'] if despacho else '',
                'estado': recepcion['tipo'] if recepcion else ('DESPACHADA' if despacho else 'EN VERIFICACION'),
                'motivo': recepcion['motivo'] if recepcion else '',
                'fecha_gestion': recepcion['fecha'] if recepcion else ''
            })
    return render_template('consultar_estado_avanzado.html', resultados=resultados)

@app.route("/exportar_estado_avanzado", methods=["POST"])
def exportar_estado_avanzado():
    numeros = request.form.get('numeros', '')
    lista_numeros = [n.strip() for n in numeros.replace('\n', ',').split(',') if n.strip()]
    data = []
    for numero in lista_numeros:
        guia = db_fetchone_dict("SELECT * FROM guias WHERE numero_guia=%s", (numero,))
        despacho = db_fetchone_dict("SELECT * FROM despachos WHERE numero_guia=%s", (numero,))
        recepcion = db_fetchone_dict("SELECT * FROM recepciones WHERE numero_guia=%s", (numero,))
        data.append({
            'numero_guia': numero,
            'remitente': guia['remitente'] if guia else '',
            'destinatario': guia['destinatario'] if guia else '',
            'direccion': guia['direccion'] if guia else '',
            'ciudad': guia['ciudad'] if guia else '',
            'mensajero': despacho['mensajero'] if despacho else '',
            'fecha_despacho': despacho['fecha'] if despacho else '',
            'estado': recepcion['tipo'] if recepcion else ('DESPACHADA' if despacho else 'EN VERIFICACION'),
            'motivo': recepcion['motivo'] if recepcion else '',
            'fecha_gestion': recepcion['fecha'] if recepcion else ''
        })
    df = pd.DataFrame(data)
    filepath = os.path.join(DATA_DIR, f'estado_avanzado.xlsx')
    df.to_excel(filepath, index=False)
    return send_file(filepath, as_attachment=True)

@app.route("/exportar_recogidas", methods=["POST"])
def exportar_recogidas():
    filtro_numero = request.form.get('filtro_numero', '').strip().lower()
    lista = recogidas
    if filtro_numero:
        lista = [r for r in recogidas if filtro_numero in (r['numero_guia'] or '').lower()]
    df = pd.DataFrame(lista)
    filepath = os.path.join(DATA_DIR, 'recogidas.xlsx')
    df.to_excel(filepath, index=False)
    return send_file(filepath, as_attachment=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
