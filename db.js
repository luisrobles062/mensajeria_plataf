const { Client } = require('pg');  // Importa el cliente de PostgreSQL

// Configura la conexión con Neon usando la URL de conexión que obtuviste de Neon
const client = new Client({
  connectionString: process.env.DATABASE_URL,  // La URL de conexión que configuraste en Render
  ssl: {
    rejectUnauthorized: false,  // Asegura la conexión SSL
  },
});

// Conéctate a la base de datos
client.connect()
  .then(() => console.log('✅ Conectado a Neon'))
  .catch(err => console.error('❌ Error de conexión', err));

// Exporta el cliente para usarlo en otros archivos de tu aplicación
module.exports = client;
