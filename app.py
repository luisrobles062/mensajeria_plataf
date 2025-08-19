from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import os
import sqlite3
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'secreto'
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

DATABASE = 'data/mensajeria.db'

# Clases para Zona y Mensajero con tarifa
class Zona:
    def __init__(self, nombre, tarifa):
        self.nombre = nombre
        self.tarifa = tarifa

class Mensajero:
    def __init__(self, nombre, zona):
        self.nombre = nombre
        self.zona = zona

# Inicializamos listas (se cargarán desde DB)
zonas = []
mensajeros = []
guias = pd.DataFrame(columns=['remitente', 'numero_guia', 'destinatario', 'direccion', 'ciudad'])
despachos = []
recepciones = []
recogidas = []

# ---------- FUNCIONES PARA BASE DE DATOS ----------

def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def cargar_datos_desde_db():
    global zonas, mensajeros, guias, despachos, recepciones, recogidas

    conn = get_db_connection()
    cur = conn.cursor()

    # Cargar zonas
    zonas = []
    for row in cur.execute('SELECT nombre, tarifa FROM zonas'):
        zonas.append(Zona(row['nombre'], row['tarifa']))

    # Cargar mensajeros
    mensajeros = []
    for row in cur.execute('SELECT nombre, zona FROM mensajeros'):
        zona_obj = next((z for z in zonas if z.nombre == row['zona']), None)
        mensajeros.append(Mensajero(row['nombre'], zona_obj))

    # Cargar guias
    guias = pd.read_sql_query('SELECT remitente, numero_guia, destinatario, direccion, ciudad FROM guias', conn)

    # Cargar despachos
    despachos = []
    for row in cur.execute('SELECT numero_guia, mensajero, zona, fecha FROM despachos'):
        despachos.append(dict(row))

    # Cargar recepciones
    recepciones = []
    for row in cur.execute('SELECT numero_guia, tipo, motivo, fecha FROM recepciones'):
        recepciones.append(dict(row))

    # Cargar recogidas
    recogidas = []
    for row in cur.execute('SELECT numero_guia, fecha, observaciones FROM recogidas'):
        recogidas.append(dict(row))

    conn.close()

def ejecutar_query(query, params=()):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    conn.close()

# Cargar datos al iniciar app
cargar_datos_desde_db()

# ---------- RUTAS ----------

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/cargar_base', methods=['GET', 'POST'])
def cargar_base():
    global guias
    if request.method == 'POST':
        archivo = request.files.get('archivo_excel')
        if archivo:
            df = pd.read_excel(archivo)
            required_cols = ['remitente', 'numero_guia', 'destinatario', 'direccion', 'ciudad']
            if all(col in df.columns for col in required_cols):
                # Guardar en base de datos (insertar filas nuevas)
                conn = get_db_connection()
                for _, row in df.iterrows():
                    # Insertar solo si no existe
                    existe = conn.execute('SELECT 1 FROM guias WHERE numero_guia = ?', (str(row['numero_guia']),)).fetchone()
                    if not existe:
                        conn.execute('INSERT INTO guias (remitente, numero_guia, destinatario, direccion, ciudad) VALUES (?, ?, ?, ?, ?)',
                                     (row['remitente'], str(row['numero_guia']), row['destinatario'], row['direccion'], row['ciudad']))
                conn.commit()
                conn.close()

                guias = pd.read_sql_query('SELECT remitente, numero_guia, destinatario, direccion, ciudad FROM guias', get_db_connection())

                archivo.save(os.path.join(DATA_DIR, archivo.filename))
                flash('Base de datos cargada correctamente.', 'success')
            else:
                flash('El archivo debe contener las columnas: ' + ", ".join(required_cols), 'danger')
    return render_template('cargar_base.html')

@app.route('/registrar_zona', methods=['GET', 'POST'])
def registrar_zona():
    if request.method == 'POST':
        nombre = request.form.get('nombre')
        tarifa = request.form.get('tarifa')
        if nombre and tarifa:
            try:
                tarifa_float = float(tarifa)
                conn = get_db_connection()
                existe = conn.execute('SELECT 1 FROM zonas WHERE nombre = ?', (nombre,)).fetchone()
                if existe:
                    flash('La zona ya existe', 'warning')
                else:
                    conn.execute('INSERT INTO zonas (nombre, tarifa) VALUES (?, ?)', (nombre, tarifa_float))
                    conn.commit()
                    flash(f'Zona {nombre} registrada con tarifa {tarifa_float}', 'success')
                conn.close()
            except ValueError:
                flash('Tarifa inválida, debe ser un número', 'danger')
        else:
            flash('Debe completar todos los campos', 'danger')
        cargar_datos_desde_db()
    return render_template('registrar_zona.html', zonas=zonas)

@app.route('/registrar_mensajero', methods=['GET', 'POST'])
def registrar_mensajero():
    if request.method == 'POST':
        nombre = request.form.get('nombre')
        zona_nombre = request.form.get('zona')
        if nombre and zona_nombre:
            conn = get_db_connection()
            zona_obj = next((z for z in zonas if z.nombre == zona_nombre), None)
            if not zona_obj:
                flash('Zona no encontrada', 'danger')
                return redirect(url_for('registrar_mensajero'))

            existe = conn.execute('SELECT 1 FROM mensajeros WHERE nombre = ?', (nombre,)).fetchone()
            if existe:
                flash('El mensajero ya existe', 'warning')
            else:
                conn.execute('INSERT INTO mensajeros (nombre, zona) VALUES (?, ?)', (nombre, zona_nombre))
                conn.commit()
                flash(f'Mensajero {nombre} registrado en zona {zona_nombre}', 'success')
            conn.close()
            cargar_datos_desde_db()
        else:
            flash('Debe completar todos los campos', 'danger')
    return render_template('registrar_mensajero.html', zonas=zonas, mensajeros=mensajeros)

@app.route('/despachar_guias', methods=['GET', 'POST'])
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

        errores = []
        exito = []

        conn = get_db_connection()

        for numero in guias_list:
            # Validar que la guía exista en la base cargada
            existe_guia = conn.execute('SELECT 1 FROM guias WHERE numero_guia = ?', (numero,)).fetchone()
            if not existe_guia:
                errores.append(f'Guía {numero} no existe (FALTANTE)')
                continue

            # Validar que no esté despachada a otro mensajero
            despacho_existente = conn.execute('SELECT * FROM despachos WHERE numero_guia = ?', (numero,)).fetchone()
            recepcion_existente = conn.execute('SELECT * FROM recepciones WHERE numero_guia = ?', (numero,)).fetchone()

            if recepcion_existente:
                errores.append(f'Guía {numero} ya fue {recepcion_existente["tipo"]}')
                continue

            if despacho_existente:
                errores.append(f'Guía {numero} ya fue despachada a {despacho_existente["mensajero"]}')
                continue

            # Insertar despacho
            conn.execute(
                'INSERT INTO despachos (numero_guia, mensajero, zona, fecha) VALUES (?, ?, ?, ?)',
                (numero, mensajero_nombre, zona_obj.nombre, fecha)
            )
            exito.append(f'Guía {numero} despachada a {mensajero_nombre}')

        conn.commit()
        conn.close()

        if errores:
            flash("Errores:<br>" + "<br>".join(errores), 'danger')
        if exito:
            flash("Despachos exitosos:<br>" + "<br>".join(exito), 'success')

        cargar_datos_desde_db()
        return redirect(url_for('ver_despacho'))

    return render_template('despachar_guias.html', mensajeros=[m.nombre for m in mensajeros], zonas=[z.nombre for z in zonas])

@app.route('/ver_despacho')
def ver_despacho():
    return render_template('ver_despacho.html', despachos=despachos)

@app.route('/registrar_recepcion', methods=['GET', 'POST'])
def registrar_recepcion():
    if request.method == 'POST':
        numero_guia = request.form.get('numero_guia')
        tipo = request.form.get('estado')  # 'ENTREGADA' o 'DEVUELTA'
        motivo = request.form.get('motivo', '')

        conn = get_db_connection()
        existe_guia = conn.execute('SELECT 1 FROM guias WHERE numero_guia = ?', (numero_guia,)).fetchone()
        if not existe_guia:
            flash('Número de guía no existe en la base (FALTANTE)', 'danger')
            conn.close()
            return redirect(url_for('registrar_recepcion'))

        despacho_existente = conn.execute('SELECT * FROM despachos WHERE numero_guia = ?', (numero_guia,)).fetchone()
        if not despacho_existente:
            flash('La guía no ha sido despachada aún', 'warning')
            conn.close()
            return redirect(url_for('registrar_recepcion'))

        recepcion_existente = conn.execute('SELECT * FROM recepciones WHERE numero_guia = ?', (numero_guia,)).fetchone()
        if recepcion_existente:
            flash('La recepción para esta guía ya está registrada', 'warning')
            conn.close()
            return redirect(url_for('registrar_recepcion'))

        fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn.execute('INSERT INTO recepciones (numero_guia, tipo, motivo, fecha) VALUES (?, ?, ?, ?)',
                     (numero_guia, tipo, motivo if tipo == 'DEVUELTA' else '', fecha))
        conn.commit()
        conn.close()

        flash(f'Recepción de guía {numero_guia} registrada como {tipo}', 'success')
        cargar_datos_desde_db()
        return redirect(url_for('registrar_recepcion'))

    return render_template('registrar_recepcion.html')

@app.route('/consultar_estado', methods=['GET', 'POST'])
def consultar_estado():
    resultado = None
    if request.method == 'POST':
        numero_guia = request.form.get('numero_guia', '').strip()
        if not numero_guia:
            flash('Debe ingresar un número de guía', 'warning')
            return redirect(url_for('consultar_estado'))

        conn = get_db_connection()
        existe_guia = conn.execute('SELECT 1 FROM guias WHERE numero_guia = ?', (numero_guia,)).fetchone()
        if not existe_guia:
            resultado = {
                'numero_guia': numero_guia,
                'estado': 'FALTANTE',
                'motivo': '',
                'mensajero': '',
                'zona': '',
                'fecha': ''
            }
        else:
            despacho = conn.execute('SELECT * FROM despachos WHERE numero_guia = ?', (numero_guia,)).fetchone()
            recepcion = conn.execute('SELECT * FROM recepciones WHERE numero_guia = ?', (numero_guia,)).fetchone()
            conn.close()

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

@app.route('/liquidacion', methods=['GET', 'POST'])
def liquidacion():
    liquidacion = None
    if request.method == 'POST':
        mensajero_nombre = request.form.get('mensajero')
        fecha_inicio = request.form.get('fecha_inicio')
        fecha_fin = request.form.get('fecha_fin')

        try:
            dt_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
            dt_fin = datetime.strptime(fecha_fin, '%Y-%m-%d')
        except Exception:
            flash('Formato de fechas inválido', 'danger')
            return redirect(url_for('liquidacion'))

        conn = get_db_connection()
        gui_despachadas = conn.execute(
            'SELECT * FROM despachos WHERE mensajero = ? AND date(fecha) BETWEEN ? AND ?',
            (mensajero_nombre, fecha_inicio, fecha_fin)
        ).fetchall()
        conn.close()

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
    return render_template('liquidacion.html', mensajeros=mensajeros, liquidacion=liquidacion)

@app.route('/registrar_recogida', methods=['GET', 'POST'])
def registrar_recogida():
    if request.method == 'POST':
        numero_guia = request.form.get('numero_guia', '').strip()
        fecha = request.form.get('fecha')
        observaciones = request.form.get('observaciones', '').strip()

        if not numero_guia or not fecha:
            flash('Debe completar número de guía y fecha', 'danger')
            return redirect(url_for('registrar_recogida'))

        # NO validamos si existe la guía en esta ruta para permitir cualquier número
        conn = get_db_connection()
        conn.execute('INSERT INTO recogidas (numero_guia, fecha, observaciones) VALUES (?, ?, ?)',
                     (numero_guia, fecha, observaciones))
        conn.commit()
        conn.close()

        flash(f'Recogida registrada para la guía {numero_guia}', 'success')
        cargar_datos_desde_db()
        return redirect(url_for('registrar_recogida'))

    return render_template('registrar_recogida.html')

@app.route('/ver_recogidas')
def ver_recogidas():
    filtro_numero = request.args.get('filtro_numero', '').strip().lower()

    if filtro_numero:
        recogidas_filtradas = [r for r in recogidas if filtro_numero in r['numero_guia'].lower()]
    else:
        recogidas_filtradas = recogidas

    return render_template('ver_recogidas.html', recogidas=recogidas_filtradas)

if __name__ == '__main__':
    app.run(debug=True)
