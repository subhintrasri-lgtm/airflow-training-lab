const sqlite3 = require('/usr/local/lib/node_modules/n8n/node_modules/sqlite3');
const db = new sqlite3.Database('/home/node/.n8n/database.sqlite', sqlite3.OPEN_READONLY);

db.all("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", (tableError, tables) => {
  if (tableError) throw tableError;
  let remaining = tables.length;
  const refs = [];
  for (const table of tables) {
    db.all(`PRAGMA foreign_key_list('${table.name.replaceAll("'", "''")}')`, (fkError, keys) => {
      if (fkError) throw fkError;
      for (const key of keys) {
        if (key.table === 'workflow_entity') refs.push({ table: table.name, ...key });
      }
      remaining -= 1;
      if (remaining === 0) {
        console.log(JSON.stringify(refs, null, 2));
        db.close();
      }
    });
  }
});
