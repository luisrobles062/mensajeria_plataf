import sqlite3

conn = sqlite3.connect('mensajeria.db')
cursor = conn.cursor()

# Crear tabla recogidas si no existe
cursor.execute('''
CREATE TABLE IF NOT EXISTS recogidas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    numero_guia TEXT NOT NULL,
    fecha TEXT NOT NULL,
    observaciones TEXT
)
''')

conn.commit()
conn.close()
print("Tabla recogidas creada o ya existente.")
