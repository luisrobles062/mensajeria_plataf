import sqlite3

conexion = sqlite3.connect('basedatos.db')
cursor = conexion.cursor()

# Tabla de guías cargadas desde Excel
cursor.execute('''
    CREATE TABLE IF NOT EXISTS guias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        remitente TEXT,
        numero_guia TEXT UNIQUE,
        destinatario TEXT,
        direccion TEXT,
        ciudad TEXT,
        zona TEXT
    )
''')

# Tabla de zonas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS zonas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT UNIQUE,
        tarifa INTEGER
    )
''')

# Tabla de mensajeros
cursor.execute('''
    CREATE TABLE IF NOT EXISTS mensajeros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT,
        zona TEXT
    )
''')

# Tabla de despachos
cursor.execute('''
    CREATE TABLE IF NOT EXISTS despachos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_guia TEXT,
        fecha TEXT,
        mensajero TEXT
    )
''')

# Tabla de estados de guías (entregado, devuelto, faltante, etc.)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS estados_guias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_guia TEXT,
        estado TEXT,
        fecha TEXT,
        causal TEXT
    )
''')

# Tabla de recogidas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS recogidas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_guia TEXT,
        fecha TEXT,
        observaciones TEXT
    )
''')

# Tabla de liquidaciones
cursor.execute('''
    CREATE TABLE IF NOT EXISTS liquidaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT,
        mensajero TEXT,
        cantidad_guias INTEGER,
        total_pesos INTEGER
    )
''')

conexion.commit()
conexion.close()

print("✅ Base de datos creada con éxito.")
