import sqlite3

conn = sqlite3.connect('mensajeria.db')
cursor = conn.cursor()

# Crear tablas básicas si no existen
cursor.execute('''
CREATE TABLE IF NOT EXISTS guias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    remitente TEXT,
    numero_guia TEXT UNIQUE,
    destinatario TEXT,
    direccion TEXT,
    ciudad TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS despachos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guia TEXT UNIQUE,
    mensajero TEXT,
    zona TEXT,
    fecha TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS recepciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    numero_guia TEXT UNIQUE,
    tipo TEXT,
    motivo TEXT,
    fecha TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS zonas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT UNIQUE,
    tarifa REAL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS mensajeros (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT UNIQUE,
    zona TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS recogidas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guia TEXT,
    fecha TEXT,
    observaciones TEXT
)
''')

# Función para verificar si una columna existe en tabla
def columna_existe(nombre_tabla, nombre_columna):
    cursor.execute(f"PRAGMA table_info({nombre_tabla})")
    columnas = [col[1] for col in cursor.fetchall()]
    return nombre_columna in columnas

# Agregar columnas si no existen
alteraciones = {
    "guias": [
        ("zona", "TEXT"),
        ("estado", "TEXT"),
        ("mensajero", "TEXT"),
        ("fecha_despacho", "TEXT"),
        ("fecha_recepcion", "TEXT"),
        ("causal", "TEXT")
    ],
    "recepciones": [
        ("estado", "TEXT"),
        ("causal", "TEXT"),
        ("fecha_recepcion", "TEXT")
    ]
}

for tabla, columnas in alteraciones.items():
    for nombre_columna, tipo_dato in columnas:
        if not columna_existe(tabla, nombre_columna):
            print(f"Agregando columna '{nombre_columna}' a la tabla '{tabla}'...")
            cursor.execute(f"ALTER TABLE {tabla} ADD COLUMN {nombre_columna} {tipo_dato}")

conn.commit()
conn.close()
print("Base de datos creada y actualizada correctamente.")
