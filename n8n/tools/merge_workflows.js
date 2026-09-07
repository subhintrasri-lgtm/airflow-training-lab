const fs = require('fs');

const basePath = 'C:/ETL/n8n_merged_delivery/n8n/workflows/';
const airflow = JSON.parse(fs.readFileSync(basePath + 'mra_airflow_webhook.json', 'utf8'));
const chat = JSON.parse(fs.readFileSync(basePath + 'mra_data_chat.json', 'utf8'));

const existingIds = new Set(airflow.nodes.map((node) => node.id));
const existingNames = new Set(airflow.nodes.map((node) => node.name));
for (const node of chat.nodes) {
  if (existingIds.has(node.id)) throw new Error('Duplicate node id: ' + node.id);
  if (existingNames.has(node.name)) throw new Error('Duplicate node name: ' + node.name);
}

const chatNodes = chat.nodes.map((node) => ({
  ...node,
  position: [node.position[0], node.position[1] + 620],
}));

airflow.name = 'MRA - Airflow Quality + AI Chat';
airflow.active = true;
airflow.nodes = [...airflow.nodes, ...chatNodes];
airflow.connections = { ...airflow.connections, ...chat.connections };
airflow.meta = { ...(airflow.meta ?? {}), templateCredsSetupCompleted: false };

fs.writeFileSync(
  basePath + 'mra_airflow_webhook.json',
  JSON.stringify(airflow, null, 2) + '\n',
  'utf8',
);
