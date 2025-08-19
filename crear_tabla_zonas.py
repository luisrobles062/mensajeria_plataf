import sqlite3

# Conecta con la base de datos principal
conn = sqlite3.connect("mensajeria.db")
cur = conn.cursor()

# Crear tabla zonas si no existe
cur.execute("""
    CREATE TABLE IF NOT EXISTS zonas (
        nombre TEXT PRIMARY KEY,
        tarifa REAL
    )
""")

print("Tabla 'zonas' creada o ya existente.")

conn.commit()
conn.close()
