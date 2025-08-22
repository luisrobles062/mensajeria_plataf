import os
import logging
from datetime import datetime
from contextlib import contextmanager
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import io

import pandas as pd
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file

app = Flask(__name__)
app.secret_key = 'secreto'  # cámbiala a var de entorno si quieres
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

# =========================
#  Conexión a Postgres/Neon
# =========================

def normalize_db_url(raw_url: str) -> str:
    """
    - Asegura sslmode=require.
    - Elimina channel_binding.
    - Agrega application_name.
    """
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
    raise RuntimeError("DATABASE_URL no está definida. En Render pon tu URL POOLER de Neon en Environment Variables.")

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
#   Esquema (si no existe)
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
            zona   TEXT REFERENCES zonas(nombre)
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS guias (
            remitente    TEXT,
            numero_guia  TEXT PRIMARY KEY,
            destinatario TEXT,
            direccion    TEXT,
            ciudad       TEXT
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS despachos (
            numero_guia TEXT PRIMARY KEY,
            mensajero   TEXT,
            zona        TEXT,
            fecha       TIMESTAMPTZ NOT NULL
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS recepciones (
            numero_guia TEXT PRIMARY KEY,
            tipo        TEXT,           -- ENTREGADA / DEVUELTA
            motivo      TEXT,
            fecha       TIMESTAMPTZ NOT NULL
        );
    """)
    db_exec("""
        CREATE TABLE IF NOT EXISTS recogidas (
            id            SERIAL PRIMARY KEY,
            numero_guia   TEXT,
            fecha         TIMESTAMPTZ NOT NULL,
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

def cargar_datos_desde_db():
    global zonas, mensajeros, guias, despachos, recepciones, recogidas

    # Zonas
    zrows = db_fetchall_dict("SELECT nombre, tarifa FROM zonas;")
    zonas = [Zona(r["nombre"], r["tarifa"]) for r in zrows]

    # Mensajeros (vincular objeto zona)
    mrows = db_fetchall_dict("SELECT nombre, zona FROM mensajeros;")
    zonas_map = {z.nombre: z for z in zonas}
    mensajeros = []
    for r in mrows:
        zona_obj = zonas_map.get(r["zona"])
        mensajeros.append(Mensajero(r["nombre"], zona_obj))
    globals()["mensajeros"] = mensajeros

    # Guías (DataFrame)
    guias = read_sql_df("SELECT remitente, numero_guia, destinatario, direccion, ciudad FROM guias;")
    globals()["guias"] = guias

    # Despachos / Recepciones / Recogidas
    despachos = db_fetchall_dict("SELECT numero_guia, mensajero, zona, fecha FROM despachos ORDER BY fecha DESC;")
    recepciones = db_fetchall_dict("SELECT numero_guia, tipo, motivo, fecha FROM recepciones ORDER BY fecha DESC;")
    recogidas = db_fetchall_dict("SELECT numero_guia, fecha, observaciones FROM recogidas ORDER BY fecha DESC;")
    globals()["despachos"] = despachos
    globals()["recepciones"] = recepciones
    globals()["recogidas"] = recogidas

# Inicializa
logging.basicConfig(level=logging.INFO)
ensure_schema()
cargar_datos_desde_db()

# =========================
#          Rutas
# =========================

@app.route("/")
def index():
    return render_template('index.html')

# ====== CONSULTAR PENDIENTES POR MENSAJERO ======
@app.route("/consultar_pendientes", methods=["GET", "POST"])
def consultar_pendientes():
    resultados = []
    if request.method == 'POST':
        mensajero_nombre = request.form.get('mensajero')
        fecha_inicio = request.form.get('fecha_inicio')
        fecha_fin = request.form.get('fecha_fin')
        query = (
            """
            SELECT g.remitente, g.numero_guia, g.destinatario, g.direccion, g.ciudad,
                   d.mensajero, d.fecha AS fecha_despacho
            FROM guias g
            JOIN despachos d ON g.numero_guia = d.numero_guia
            WHERE d.mensajero = %s
              AND d.fecha::date BETWEEN %s AND %s
              AND NOT EXISTS (
                    SELECT 1 FROM recepciones r
                    WHERE r.numero_guia = g.numero_guia
              )
            ORDER BY d.fecha DESC;
            """
        )
        resultados = db_fetchall_dict(query, (mensajero_nombre, fecha_inicio, fecha_fin))
    # pasamos nombres de mensajeros para el select
    return render_template('consultar_pendientes.html', resultados=resultados, mensajeros=[m.nombre for m in mensajeros])

@app.route("/exportar_pendientes", methods=["POST"])
def exportar_pendientes():
    mensajero_nombre = request.form.get('mensajero')
    fecha_inicio = request.form.get('fecha_inicio')
    fecha_fin = request.form.get('fecha_fin')
    query = (
        """
        SELECT g.remitente, g.numero_guia, g.destinatario, g.direccion, g.ciudad,
               d.mensajero, d.fecha AS fecha_despacho
        FROM guias g
        JOIN despachos d ON g.numero_guia = d.numero_guia
        WHERE d.mensajero = %s
          AND d.fecha::date BETWEEN %s AND %s
          AND NOT EXISTS (
                SELECT 1 FROM recepciones r
                WHERE r.numero_guia = g.numero_guia
          )
        ORDER BY d.fecha DESC;
        """
    )
    df = read_sql_df(query, (mensajero_nombre, fecha_inicio, fecha_fin))
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Pendientes')
    output.seek(0)
    return send_file(output, download_name="pendientes_mensajero.xlsx", as_attachment=True)

# ====== CONSULTAR PENDIENTES GENERALES ======
@app.route("/consultar_pendientes_general", methods=["GET", "POST"])
def consultar_pendientes_general():
    resultados = []
    if request.method == 'POST':
        fecha_inicio = request.form.get('fecha_inicio')
        fecha_fin = request.form.get('fecha_fin')
        query = (
            """
            SELECT g.remitente, g.numero_guia, g.destinatario, g.direccion, g.ciudad,
                   d.mensajero, d.fecha AS fecha_despacho
            FROM guias g
            JOIN despachos d ON g.numero_guia = d.numero_guia
            WHERE d.fecha::date BETWEEN %s AND %s
              AND NOT EXISTS (
                    SELECT 1 FROM recepciones r
                    WHERE r.numero_guia = g.numero_guia
              )
            ORDER BY d.fecha DESC;
            """
        )
        resultados = db_fetchall_dict(query, (fecha_inicio, fecha_fin))
    return render_template('consultar_pendientes_general.html', resultados=resultados)

@app.route("/exportar_pendientes_general", methods=["POST"])
def exportar_pendientes_general():
    fecha_inicio = request.form.get('fecha_inicio')
    fecha_fin = request.form.get('fecha_fin')
    query = (
        """
        SELECT g.remitente, g.numero_guia, g.destinatario, g.direccion, g.ciudad,
               d.mensajero, d.fecha AS fecha_despacho
        FROM guias g
        JOIN despachos d ON g.numero_guia = d.numero_guia
        WHERE d.fecha::date BETWEEN %s AND %s
          AND NOT EXISTS (
                SELECT 1 FROM recepciones r
                WHERE r.numero_guia = g.numero_guia
          )
        ORDER BY d.fecha DESC;
        """
    )
    df = read_sql_df(query, (fecha_inicio, fecha_fin))
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='PendientesGeneral')
    output.seek(0)
    return send_file(output, download_name="pendientes_general.xlsx", as_attachment=True)

# ====== CONSULTA DE ESTADO (tu existente) ======
@app.route("/consultar_estado", methods=["GET", "POST"])
def consultar_estado():
    resultado = None
    if request.method == 'POST':
        numero_guia = request.form.get('numero_guia', '').strip()
        if not numero_guia:
            flash('Debe ingresar un número de guía', 'warning')
            return redirect(url_for('consultar_estado'))

        existe_guia = db_fetchone_dict("SELECT 1 AS x FROM guias WHERE numero_guia = %s;", (numero_guia,))
        if not existe_guia:
            resultado = {'numero_guia': numero_guia, 'estado': 'FALTANTE', 'motivo': '', 'mensajero': '', 'zona': '', 'fecha': ''}
        else:
            despacho = db_fetchone_dict("SELECT * FROM despachos WHERE numero_guia = %s;", (numero_guia,))
            recepcion = db_fetchone_dict("SELECT * FROM recepciones WHERE numero_guia = %s;", (numero_guia,))

            if recepcion:
                estado = recepcion['tipo']
                motivo = recepcion['motivo']
                mensajero = despacho['mensajero'] if despacho else ''
                zona = despacho['zona'] if despacho else ''
                fecha = recepcion['fecha']
            elif despacho:
                estado = 'DESPACHADA'
                motivo = ''
                mensajero = despacho['mensajero']
                zona = despacho['zona']
                fecha = despacho['fecha']
            else:
                estado = 'EN VERIFICACION'
                motivo = ''
                mensajero = ''
                zona = ''
                fecha = ''

            resultado = {
                'numero_guia': numero_guia,
                'estado': estado,
                'motivo': motivo,
                'mensajero': mensajero,
                'zona': zona,
                'fecha': fecha
            }
    return render_template('consultar_estado.html', resultado=resultado)

# ====== CONSULTA DE ESTADO AVANZADA (múltiples guías) ======
@app.route("/consultar_estado_avanzado", methods=["GET", "POST"])
def consultar_estado_avanzado():
    resultados = []
    if request.method == 'POST':
        numeros = request.form.get('numeros', '')
        lista_numeros = [n.strip() for n in numeros.replace("
", ",").split(",") if n.strip()]
        if not lista_numeros:
            flash('Debe ingresar al menos un número de guía', 'warning')
            return redirect(url_for('consultar_estado_avanzado'))
        placeholders = ",".join(["%s"] * len(lista_numeros))
        query = f"""
            SELECT g.numero_guia, g.remitente, g.destinatario, g.direccion, g.ciudad,
                   d.mensajero, d.fecha AS fecha_despacho,
                   COALESCE(r.tipo, CASE WHEN d.numero_guia IS NOT NULL THEN 'DESPACHADA' ELSE 'EN VERIFICACION' END) AS estado,
                   r.motivo, r.fecha AS fecha_gestion
            FROM guias g
            LEFT JOIN despachos d ON g.numero_guia = d.numero_guia
            LEFT JOIN recepciones r ON g.numero_guia = r.numero_guia
            WHERE g.numero_guia IN ({placeholders})
            ORDER BY d.fecha DESC NULLS LAST;
        """
        resultados = db_fetchall_dict(query, tuple(lista_numeros))
    return render_template('consultar_estado_avanzado.html', resultados=resultados)

@app.route("/exportar_estado_avanzado", methods=["POST"])
def exportar_estado_avanzado():
    numeros = request.form.get('numeros', '')
    lista_numeros = [n.strip() for n in numeros.replace("
", ",").split(",") if n.strip()]
    if not lista_numeros:
        flash('Debe ingresar al menos un número de guía', 'warning')
        return redirect(url_for('consultar_estado_avanzado'))
    placeholders = ",".join(["%s"] * len(lista_numeros))
    query = f"""
        SELECT g.numero_guia, g.remitente, g.destinatario, g.direccion, g.ciudad,
               d.mensajero, d.fecha AS fecha_despacho,
               COALESCE(r.tipo, CASE WHEN d.numero_guia IS NOT NULL THEN 'DESPACHADA' ELSE 'EN VERIFICACION' END) AS estado,
               r.motivo, r.fecha AS fecha_gestion
        FROM guias g
        LEFT JOIN despachos d ON g.numero_guia = d.numero_guia
        LEFT JOIN recepciones r ON g.numero_guia = r.numero_guia
        WHERE g.numero_guia IN ({placeholders})
        ORDER BY d.fecha DESC NULLS LAST;
    """
    df = read_sql_df(query, tuple(lista_numeros))
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='EstadoGuias')
    output.seek(0)
    return send_file(output, download_name="estado_guias.xlsx", as_attachment=True)

# ====== RUTAS EXISTENTES (cargar base, zonas, mensajeros, despacho, recepciones, liquidación, recogidas) ======
@app.route("/cargar_base", methods=["GET", "POST"])
def cargar_base():
    global guias
    if request.method == 'POST':
        archivo = request.files.get('archivo_excel')
        if archivo:
            df = pd.read_excel(archivo)
            required_cols = ['remitente', 'numero_guia', 'destinatario', 'direccion', 'ciudad']
            if all(col in df.columns for col in required_cols):
                with get_conn() as conn:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        numero = str(row['numero_guia'])
                        cur.execute("SELECT 1 FROM guias WHERE numero_guia = %s;", (numero,))
                        existe = cur.fetchone()
                        if not existe:
                            cur.execute(
                                """
                                INSERT INTO guias(remitente, numero_guia, destinatario, direccion, ciudad)
                                VALUES (%s, %s, %s, %s, %s);
                                """,
                                (row['remitente'], numero, row['destinatario'], row['direccion'], row['ciudad'])
                            )
                guias = read_sql_df("SELECT remitente, numero_guia, destinatario, direccion, ciudad FROM guias;")
                globals()["guias"] = guias
                archivo.save(os.path.join(DATA_DIR, archivo.filename))
                flash('Base de datos cargada correctamente.', 'success')
            else:
                flash('El archivo debe contener las columnas: ' + ", ".join(required_cols), 'danger')
    return render_template('cargar_base.html')

@app.route("/registrar_zona", methods=["GET", "POST"])
def registrar_zona():
    if request.method == 'POST':
        nombre = request.form.get('nombre')
        tarifa = request.form.get('tarifa')
        if nombre and tarifa:
            try:
                tarifa_float = float(tarifa)
                existe = db_fetchone_dict("SELECT 1 AS x FROM zonas WHERE nombre = %s;", (nombre,))
                if existe:
                    flash('La zona ya existe', 'warning')
                else:
                    db_exec("INSERT INTO zonas(nombre, tarifa) VALUES (%s, %s);", (nombre, tarifa_float))
                    flash(f'Zona {nombre} registrada con tarifa {tarifa_float}', 'success')
                cargar_datos_desde_db()
            except ValueError:
                flash('Tarifa inválida, debe ser un número', 'danger')
        else:
            flash('Debe completar todos los campos', 'danger')
    return render_template('registrar_zona.html', zonas=zonas)

@app.route("/registrar_mensajero", methods=["GET", "POST"])
def registrar_mensajero():
    if request.method == 'POST':
        nombre = request.form.get('nombre')
        zona_nombre = request.form.get('zona')
        if nombre and zona_nombre:
            zona_obj = next((z for z in zonas if z.nombre == zona_nombre), None)
            if not zona_obj:
                flash('Zona no encontrada', 'danger')
                return redirect(url_for('registrar_mensajero'))

            existe = db_fetchone_dict("SELECT 1 AS x FROM mensajeros WHERE nombre = %s;", (nombre,))
            if existe:
                flash('El mensajero ya existe', 'warning')
            else:
                db_exec("INSERT INTO mensajeros(nombre, zona) VALUES (%s, %s);", (nombre, zona_nombre))
                flash(f'Mensajero {nombre} registrado en zona {zona_nombre}', 'success')
            cargar_datos_desde_db()
        else:
            flash('Debe completar todos los campos', 'danger')
    return render_template('registrar_mensajero.html', zonas=zonas, mensajeros=mensajeros)

@app.route("/despachar_guias", methods=["GET", "POST"])
def despachar_guias():
    if request.method == 'POST':
        mensajero_nombre = request.form.get('mensajero')
        guias_input = request.form.get('guias', '')
        guias_list = [g.strip() for g in guias_input.strip().splitlines() if g.strip()]
        fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        mensajero_obj = next((m for m in mensajeros if m.nombre == mensajero_nombre), None)

        if not mensajero_obj:
            flash('Mensajero no encontrado', 'danger')
            return redirect(url_for('despachar_guias'))

        zona_obj = mensajero_obj.zona
        errores, exito = [], []

        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            for numero in guias_list:
                # guía existe
                cur.execute("SELECT 1 FROM guias WHERE numero_guia = %s;", (numero,))
                if not cur.fetchone():
                    errores.append(f'Guía {numero} no existe (FALTANTE)')
                    continue
                # ya recepcionada?
                cur.execute("SELECT * FROM recepciones WHERE numero_guia = %s;", (numero,))
                recepcion_existente = cur.fetchone()
                if recepcion_existente:
                    errores.append(f'Guía {numero} ya fue {recepcion_existente["tipo"]}')
                    continue
                # ya despachada?
                cur.execute("SELECT * FROM despachos WHERE numero_guia = %s;", (numero,))
                despacho_existente = cur.fetchone()
                if despacho_existente:
                    errores.append(f'Guía {numero} ya fue despachada a {despacho_existente["mensajero"]}')
                    continue
                # insertar
                cur.execute(
                    """
                    INSERT INTO despachos(numero_guia, mensajero, zona, fecha)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (numero, mensajero_nombre, zona_obj.nombre if zona_obj else None, fecha)
                )
                exito.append(f'Guía {numero} despachada a {mensajero_nombre}')

        if errores:
            flash("Errores:<br>" + "<br>".join(errores), 'danger')
        if exito:
            flash("Despachos exitosos:<br>" + "<br>".join(exito), 'success')

        cargar_datos_desde_db()
        return redirect(url_for('ver_despacho'))

    return render_template('despachar_guias.html',
                           mensajeros=[m.nombre for m in mensajeros],
                           zonas=[z.nombre for z in zonas])

@app.route("/ver_despacho")
def ver_despacho():
    return render_template('ver_despacho.html', despachos=despachos)

@app.route("/registrar_recepcion", methods=["GET", "POST"])
def registrar_recepcion():
    if request.method == 'POST':
        numero_guia = request.form.get('numero_guia')
        tipo = request.form.get('estado')  # ENTREGADA o DEVUELTA
        motivo = request.form.get('motivo', '')
        fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # guía existe?
        existe_guia = db_fetchone_dict("SELECT 1 AS x FROM guias WHERE numero_guia = %s;", (numero_guia,))
        if not existe_guia:
            flash('Número de guía no existe en la base (FALTANTE)', 'danger')
            return redirect(url_for('registrar_recepcion'))

        despacho_existente = db_fetchone_dict("SELECT * FROM despachos WHERE numero_guia = %s;", (numero_guia,))
        if not despacho_existente:
            flash('La guía no ha sido despachada aún', 'warning')
            return redirect(url_for('registrar_recepcion'))

        recepcion_existente = db_fetchone_dict("SELECT 1 AS x FROM recepciones WHERE numero_guia = %s;", (numero_guia,))
        if recepcion_existente:
            flash('La recepción para esta guía ya está registrada', 'warning')
            return redirect(url_for('registrar_recepcion'))

        db_exec(
            """
            INSERT INTO recepciones(numero_guia, tipo, motivo, fecha)
            VALUES (%s, %s, %s, %s);
            """,
            (numero_guia, tipo, (motivo if tipo == 'DEVUELTA' else ''), fecha)
        )

        flash(f'Recepción de guía {numero_guia} registrada como {tipo}', 'success')
        cargar_datos_desde_db()
        return redirect(url_for('registrar_recepcion'))

    return render_template('registrar_recepcion.html')

@app.route("/liquidacion", methods=["GET", "POST"])
def liquidacion():
    liquidacion = None
    if request.method == 'POST':
        mensajero_nombre = request.form.get('mensajero')
        fecha_inicio = request.form.get('fecha_inicio')
        fecha_fin = request.form.get('fecha_fin')

        try:
            datetime.strptime(fecha_inicio, '%Y-%m-%d')
            datetime.strptime(fecha_fin, '%Y-%m-%d')
        except Exception:
            flash('Formato de fechas inválido', 'danger')
            return redirect(url_for('liquidacion'))

        gui_despachadas = db_fetchall_dict(
            'SELECT * FROM despachos WHERE mensajero = %s AND DATE(fecha) BETWEEN %s AND %s;',
            (mensajero_nombre, fecha_inicio, fecha_fin)
        )

        cantidad_guias = len(gui_despachadas)
        mensajero_obj = next((m for m in mensajeros if m.nombre == mensajero_nombre), None)
        tarifa = mensajero_obj.zona.tarifa if mensajero_obj and mensajero_obj.zona else 0
        total_pagar = cantidad_guias * tarifa

        liquidacion = {
            'mensajero': mensajero_nombre,
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin,
            'cantidad_guias': cantidad_guias,
            'total_pagar': total_pagar
        }
    return render_template('liquidacion.html', mensajeros=[m.nombre for m in mensajeros], liquidacion=liquidacion)

@app.route("/registrar_recogida", methods=["GET", "POST"])
def registrar_recogida():
    if request.method == 'POST':
        numero_guia = request.form.get('numero_guia', '').strip()
        fecha = request.form.get('fecha')
        observaciones = request.form.get('observaciones', '').strip()

        if not numero_guia or not fecha:
            flash('Debe completar número de guía y fecha', 'danger')
            return redirect(url_for('registrar_recogida'))

        db_exec(
            """
            INSERT INTO recogidas(numero_guia, fecha, observaciones)
            VALUES (%s, %s, %s);
            """,
            (numero_guia, fecha, observaciones)
        )

        flash(f'Recogida registrada para la guía {numero_guia}', 'success')
        cargar_datos_desde_db()
        return redirect(url_for('registrar_recogida'))

    return render_template('registrar_recogida.html')

@app.route("/ver_recogidas")
def ver_recogidas():
    filtro_numero = request.args.get('filtro_numero', '').strip().lower()
    lista = recogidas
    if filtro_numero:
        lista = [r for r in recogidas if filtro_numero in (r['numero_guia'] or '').lower()]
    return render_template('ver_recogidas.html', recogidas=lista, filtro_numero=filtro_numero)

# ====== EXPORTAR RECOGIDAS ======
@app.route("/exportar_recogidas", methods=["POST"])
def exportar_recogidas():
    filtro_numero = request.form.get('filtro_numero', '').strip().lower()
    query = (
        """
        SELECT numero_guia, fecha, observaciones
        FROM recogidas
        WHERE (%s = '' OR LOWER(numero_guia) LIKE '%' || %s || '%')
        ORDER BY fecha DESC;
        """
    )
    params = (filtro_numero, filtro_numero)
    df = read_sql_df(query, params)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Recogidas')
    output.seek(0)
    return send_file(output, download_name="recogidas.xlsx", as_attachment=True)

# ====== Endpoints utilitarios ======
@app.route("/health")
def health():
    db_fetchone_dict("SELECT 1 AS ok;")
    return jsonify(ok=True)

@app.route("/init")
def init():
    ensure_schema()
    new_id = db_fetchone_dict(
        "INSERT INTO guias(remitente, numero_guia, destinatario, direccion, ciudad) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (numero_guia) DO NOTHING RETURNING numero_guia;",
        ("demo", "DUMMY-0001", "destino", "direccion", "ciudad")
    )
    return jsonify(ok=True, demo_insert=new_id)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
