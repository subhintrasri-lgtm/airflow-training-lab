const sqlite3 = require('/usr/local/lib/node_modules/n8n/node_modules/sqlite3');
const db = new sqlite3.Database('/home/node/.n8n/database.sqlite', sqlite3.OPEN_READONLY);
db.all('SELECT id, name, type FROM credentials_entity ORDER BY name', (error, rows) => {
  if (error) throw error;
  console.log(JSON.stringify(rows, null, 2));
  db.close();
});
