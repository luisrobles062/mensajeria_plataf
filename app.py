from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os

app = Flask(__name__)
app.secret_key = 'secreto'

# ---------------------------
# Conexión a la base de datos Neon PostgreSQL
# ---------------------------
DATABASE_URL = "postgresql://neondb_owner:npg_cjyR1qu3gYLv@ep-sweet-moon-adescaz8-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn

# ---------------------------
# Página principal
# ---------------------------
@app.route('/')
def index():
    return render_template('index.html')

# ---------------------------
# Cargar base de guías
# ---------------------------
@app.route('/cargar_base', methods=['GET', 'POST'])
def cargar_base():
    if request.method == 'POST':
        file = request.files['archivo']
        if not file:
            flash("No se seleccionó archivo", "danger")
            return redirect(request.url)
        df = pd.read_excel(file)
        # Guardar en la tabla "guias"
        conn = get_conn()
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO guias (remitente, numero_guia, destinatario, direccion, ciudad, estado)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (numero_guia) DO NOTHING
            """, (row['remitente'], row['numero_guia'], row['destinatario'], row['direccion'], row['ciudad'], 'Pendiente'))
        conn.commit()
        cur.close()
        conn.close()
        flash("Base cargada correctamente", "success")
        return redirect(url_for('index'))
    return render_template('cargar_base.html')

# ---------------------------
# Registrar zona
# ---------------------------
@app.route('/registrar_zona', methods=['GET', 'POST'])
def registrar_zona():
    if request.method == 'POST':
        nombre = request.form['nombre']
        tarifa = float(request.form['tarifa'])
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO zonas (nombre, tarifa) VALUES (%s,%s) ON CONFLICT (nombre) DO NOTHING", (nombre, tarifa))
        conn.commit()
        cur.close()
        conn.close()
        flash("Zona registrada", "success")
        return redirect(url_for('index'))
    return render_template('registrar_zona.html')

# ---------------------------
# Registrar mensajero
# ---------------------------
@app.route('/registrar_mensajero', methods=['GET', 'POST'])
def registrar_mensajero():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM zonas")
    zonas = cur.fetchall()
    cur.close()
    conn.close()
    if request.method == 'POST':
        nombre = request.form['nombre']
        zona_id = request.form['zona']
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO mensajeros (nombre, zona_id) VALUES (%s,%s) ON CONFLICT (nombre) DO NOTHING", (nombre, zona_id))
        conn.commit()
        cur.close()
        conn.close()
        flash("Mensajero registrado", "success")
        return redirect(url_for('index'))
    return render_template('registrar_mensajero.html', zonas=zonas)

# ---------------------------
# Despachar guías
# ---------------------------
@app.route('/despachar_guias', methods=['GET', 'POST'])
def despachar_guias():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM mensajeros")
    mensajeros = cur.fetchall()
    cur.close()
    conn.close()
    if request.method == 'POST':
        mensajero_id = request.form['mensajero']
        numeros = request.form['numeros']  # números separados por coma
        lista_numeros = [n.strip() for n in numeros.split(',')]
        conn = get_conn()
        cur = conn.cursor()
        for numero in lista_numeros:
            cur.execute("UPDATE guias SET estado='Despachada', mensajero_id=%s WHERE numero_guia=%s AND estado='Pendiente'", (mensajero_id, numero))
        conn.commit()
        cur.close()
        conn.close()
        flash("Guías despachadas", "success")
        return redirect(url_for('index'))
    return render_template('despachar_guias.html', mensajeros=mensajeros)

# ---------------------------
# Registrar recepción
# ---------------------------
@app.route('/registrar_recepcion', methods=['GET', 'POST'])
def registrar_recepcion():
    if request.method == 'POST':
        numero = request.form['numero']
        estado = request.form['estado']
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE guias SET estado=%s WHERE numero_guia=%s", (estado, numero))
        conn.commit()
        cur.close()
        conn.close()
        flash("Recepción registrada", "success")
        return redirect(url_for('index'))
    return render_template('registrar_recepcion.html')

# ---------------------------
# Consultar estado
# ---------------------------
@app.route('/consultar_estado', methods=['GET', 'POST'])
def consultar_estado():
    guia = None
    if request.method == 'POST':
        numero = request.form['numero']
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM guias WHERE numero_guia=%s", (numero,))
        guia = cur.fetchone()
        cur.close()
        conn.close()
    return render_template('consultar_estado.html', guia=guia)

# ---------------------------
# Liquidación mensajero
# ---------------------------
@app.route('/liquidacion')
def liquidacion():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT m.nombre as mensajero, count(g.id) as total_guias, sum(z.tarifa) as total
        FROM guias g
        JOIN mensajeros m ON g.mensajero_id = m.id
        JOIN zonas z ON m.zona_id = z.id
        WHERE g.estado='Despachada'
        GROUP BY m.nombre, z.tarifa
    """)
    datos = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('liquidacion.html', datos=datos)

# ---------------------------
# Registrar recogida
# ---------------------------
@app.route('/registrar_recogida', methods=['GET', 'POST'])
def registrar_recogida():
    if request.method == 'POST':
        numero = request.form['numero']
        fecha = request.form['fecha']
        observaciones = request.form['observaciones']
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO recogidas (numero_guia, fecha, observaciones) VALUES (%s,%s,%s)", (numero, fecha, observaciones))
        conn.commit()
        cur.close()
        conn.close()
        flash("Recogida registrada", "success")
        return redirect(url_for('index'))
    return render_template('registrar_recogida.html')

# ---------------------------
# Ver recogidas
# ---------------------------
@app.route('/ver_recogidas')
def ver_recogidas():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM recogidas ORDER BY fecha DESC")
    datos = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('ver_recogidas.html', datos=datos)

if __name__ == '__main__':
    app.run(debug=True)
