const fs = require('fs');

const guardCode = String.raw`const AI_ENABLED = false;

const trigger = $('MRA Data Chat').first().json;
const question = String(trigger.chatInput ?? trigger.query ?? '').trim();
const sessionId = String(trigger.sessionId ?? 'mra-local-chat');
const source = $input.first().json;
const payload = source.payload ?? source.data ?? source;

const forbiddenKeys = new Set([
  'hn', 'an', 'patientname', 'patient_name', 'citizenid', 'citizen_id',
  'nationalid', 'national_id', 'firstname', 'lastname', 'ชื่อผู้ป่วย', 'เลขบัตรประชาชน'
]);

function findForbiddenKey(value, path = 'root') {
  if (!value || typeof value !== 'object') return null;
  for (const [key, child] of Object.entries(value)) {
    const normalized = String(key).trim().toLowerCase().replace(/[^a-z0-9_ก-๙]/g, '');
    if (forbiddenKeys.has(normalized)) return path + '.' + key;
    const nested = findForbiddenKey(child, path + '.' + key);
    if (nested) return nested;
  }
  return null;
}

const sensitiveQuestion = /\b(?:HN|AN)\b|ชื่อผู้ป่วย|เลขบัตร|ข้อมูลส่วนบุคคล|รายบุคคล|ผู้ป่วยราย|คนไข้ราย/i.test(question);
const forbiddenPath = findForbiddenKey(payload);
const privacyPassed = payload?.privacy?.contains_patient_identifiers === false;
const quality = payload?.quality_summary ?? {};
const payloadValid = Boolean(payload?.schema_version) && Array.isArray(payload?.aggregates) && privacyPassed && !forbiddenPath;

const rows = Array.isArray(payload?.aggregates) ? payload.aggregates : [];
const validRows = rows.filter(row => Number.isFinite(Number(row.completion_rate)));
const roleMap = new Map();
for (const row of validRows) {
  const role = row.Role ?? 'Unknown';
  const current = roleMap.get(role) ?? { role, scored: 0, complete: 0, incomplete: 0 };
  current.scored += Number(row.scored ?? 0);
  current.complete += Number(row.complete ?? 0);
  current.incomplete += Number(row.incomplete ?? 0);
  roleMap.set(role, current);
}

const roleSummary = [...roleMap.values()].map(item => ({
  ...item,
  completion_rate: item.scored > 0 ? Number((item.complete / item.scored).toFixed(4)) : null,
})).sort((a, b) => (a.completion_rate ?? 1) - (b.completion_rate ?? 1));

const worstAreas = [...validRows]
  .sort((a, b) => Number(a.completion_rate) - Number(b.completion_rate))
  .slice(0, 20)
  .map(row => ({
    role: row.Role,
    period: row['Year-Month'],
    dimension: row.QDimension ?? 'ไม่ระบุ',
    scored: Number(row.scored ?? 0),
    complete: Number(row.complete ?? 0),
    incomplete: Number(row.incomplete ?? 0),
    completion_rate: Number(Number(row.completion_rate).toFixed(4)),
  }));

const warnings = (payload?.checks ?? []).filter(check => check.status === 'WARN').map(check => ({
  check_id: check.check_id,
  scope: check.scope,
  message_th: check.message_th,
  evidence: check.evidence,
}));

let blockedMessage = '';
if (sensitiveQuestion) {
  blockedMessage = 'ขออภัยค่ะ ระบบนี้ตอบได้เฉพาะข้อมูลคุณภาพแบบสรุป (Aggregate) และไม่สามารถค้นหา แสดง หรืออนุมาน HN, AN ชื่อผู้ป่วย หรือข้อมูลรายบุคคลได้ กรุณาถามเป็นระดับวิชาชีพ เดือน หรือมิติคุณภาพแทนค่ะ';
} else if (!payloadValid) {
  blockedMessage = 'ยังไม่สามารถตอบได้อย่างปลอดภัยค่ะ เพราะไม่พบ Agent payload ที่ผ่านการตรวจ Privacy กรุณารัน DAG mra_agentic_pipeline แล้วลองใหม่';
}

const context = {
  run_id: payload?.run_id ?? null,
  quality_summary: quality,
  metric_definition: payload?.metric_definition ?? {},
  role_summary: roleSummary,
  worst_20_role_month_dimensions: worstAreas,
  warnings,
  privacy: payload?.privacy ?? {},
};

const agentPrompt = [
  'คำถามผู้ใช้: ' + question,
  '',
  'ตอบโดยใช้ข้อมูล Aggregate ด้านล่างเท่านั้น หากข้อมูลไม่เพียงพอให้บอกว่าไม่มีข้อมูล ห้ามสร้างตัวเลขหรือสาเหตุเป็นข้อเท็จจริง',
  'เมื่อกล่าวถึงสาเหตุที่ยังไม่ยืนยัน ให้ใช้คำว่า “สมมติฐาน” และเสนอวิธีตรวจสอบ',
  'ห้ามขอ แสดง หรืออนุมาน HN, AN ชื่อผู้ป่วย หรือข้อมูลส่วนบุคคล และห้ามตัดสินใจทางคลินิกแทนมนุษย์',
  'ตอบภาษาไทย กระชับ ระบุตัวเลขที่ใช้ และปิดท้ายด้วยข้อจำกัด/Human Review เมื่อให้ข้อเสนอแนะ',
  '',
  'บริบทข้อมูล:',
  JSON.stringify(context),
].join('\n');

return [{ json: {
  allowed: !sensitiveQuestion && payloadValid,
  ai_enabled: AI_ENABLED,
  question,
  sessionId,
  blocked_message: blockedMessage,
  context,
  agent_prompt: agentPrompt,
} }];`;

const deterministicCode = String.raw`const data = $input.first().json;
const question = String(data.question ?? '').toLowerCase();
const context = data.context ?? {};
const quality = context.quality_summary ?? {};
const roles = context.role_summary ?? [];
const worst = context.worst_20_role_month_dimensions ?? [];
const warnings = context.warnings ?? [];

const pct = value => Number.isFinite(Number(value)) ? (Number(value) * 100).toFixed(2) + '%' : 'ไม่พบข้อมูล';
const number = value => Number(value ?? 0).toLocaleString('th-TH');
const areaLine = (area, index) => (index + 1) + '. ' + area.role + ' | ' + area.period + ' | มิติ ' + area.dimension + ' = ' + pct(area.completion_rate) + ' (ไม่ครบ ' + number(area.incomplete) + '/' + number(area.scored) + ')';
const roleLine = (role, index) => (index + 1) + '. ' + role.role + ' = ' + pct(role.completion_rate) + ' (ไม่ครบ ' + number(role.incomplete) + '/' + number(role.scored) + ')';

let lines = ['โหมดกฎสำรอง (ยังไม่ได้เปิด Gemini AI)'];
if (/ต่ำ|แย่|ผิดปกติ|น้อยสุด|bottom|worst/.test(question)) {
  lines.push('จุดที่ completion rate ต่ำที่สุดจากข้อมูลล่าสุด:');
  lines.push(...worst.slice(0, 5).map(areaLine));
} else if (/ปรับปรุง|แก้ไข|แนะนำ|recommend|action/.test(question)) {
  lines.push('ข้อเสนอแนะที่ควรทำก่อน:');
  if (Number(quality.join_coverage_rate) < 0.98) {
    lines.push('1. ตรวจ mapping ของรายการที่ Join ไม่สำเร็จ ปัจจุบัน Join coverage ' + pct(quality.join_coverage_rate) + ' เป้าหมายอย่างน้อย 98%');
  }
  lines.push('2. ให้เจ้าของกระบวนการทบทวน 3 กลุ่มต่ำสุด:');
  lines.push(...worst.slice(0, 3).map(areaLine));
  lines.push('3. ติดตาม completion rate รอบถัดไปและยืนยันสาเหตุด้วย Human Review ก่อนดำเนินการ');
} else if (/วิชาชีพ|role|doctor|nurse|pharmacist|dietitian|physio|csr|lab|xray|แพทย์|พยาบาล|เภสัช|โภชน|กายภาพ|รังสี/.test(question)) {
  lines.push('สรุปตามวิชาชีพ เรียงจาก completion rate ต่ำไปสูง:');
  lines.push(...roles.map(roleLine));
} else if (/ข้อจำกัด|warning|เตือน|คุณภาพ|quality/.test(question)) {
  lines.push('สถานะข้อมูล: ' + (quality.readiness_status ?? 'ไม่พบข้อมูล'));
  lines.push('จำนวน Fact: ' + number(quality.fact_rows) + ' แถว');
  lines.push('Join coverage: ' + pct(quality.join_coverage_rate));
  lines.push('Failed checks: ' + number(quality.failed_checks) + ', Warning checks: ' + number(quality.warning_checks));
  if (warnings.length) {
    lines.push('คำเตือนหลัก:');
    lines.push(...warnings.slice(0, 8).map((warning, index) => (index + 1) + '. ' + warning.check_id + ' - ' + warning.message_th));
  }
} else if (/สรุป|สถานะ|ภาพรวม|ล่าสุด|summary|status/.test(question)) {
  lines.push('สถานะข้อมูลล่าสุด: ' + (quality.readiness_status ?? 'ไม่พบข้อมูล'));
  lines.push('จำนวน Fact: ' + number(quality.fact_rows) + ' แถว');
  lines.push('Join coverage: ' + pct(quality.join_coverage_rate));
  lines.push('Failed checks: ' + number(quality.failed_checks) + ', Warning checks: ' + number(quality.warning_checks));
  if (worst[0]) lines.push('จุดต่ำสุด: ' + areaLine(worst[0], 0));
} else {
  lines.push('ฉันตอบได้จากข้อมูล Aggregate ล่าสุด เช่น:');
  lines.push('- สรุปสถานะคุณภาพข้อมูลล่าสุด');
  lines.push('- จุดที่ completion rate ต่ำที่สุด');
  lines.push('- สรุป completion rate ตามวิชาชีพ');
  lines.push('- ควรปรับปรุงอะไรเป็นลำดับแรก');
  lines.push('- มีคำเตือนหรือข้อจำกัดอะไรบ้าง');
}
lines.push('ข้อจำกัด: คำตอบนี้มาจากกฎและข้อมูลสรุป ไม่ใช่การวินิจฉัยทางคลินิก และข้อเสนอแนะต้องผ่าน Human Review');
return [{ json: { output: lines.join('\n') } }];`;

const workflow = {
  id: 'MraDataChat01',
  name: 'MRA - Chat with Quality Data',
  active: true,
  nodes: [
    {
      parameters: {
        public: true,
        mode: 'hostedChat',
        authentication: 'none',
        initialMessages: 'สวัสดีค่ะ ฉันช่วยตอบคำถามจากข้อมูลคุณภาพ MRA แบบสรุปล่าสุดได้ โดยไม่ใช้ข้อมูลผู้ป่วยรายบุคคล',
        availableInChat: true,
        agentName: 'MRA Quality Assistant',
        agentDescription: 'ผู้ช่วยวิเคราะห์ Data Quality จาก Agent payload แบบ Aggregate',
        suggestedPrompts: {
          prompts: [
            { icon: { type: 'emoji', value: '📊' }, text: 'สรุปสถานะคุณภาพข้อมูลล่าสุด' },
            { icon: { type: 'emoji', value: '⚠️' }, text: 'จุดที่ completion rate ต่ำที่สุดคืออะไร' },
            { icon: { type: 'emoji', value: '🛠️' }, text: 'ควรปรับปรุงอะไรเป็นลำดับแรก' },
            { icon: { type: 'emoji', value: '🔎' }, text: 'มีข้อจำกัดของข้อมูลอะไรบ้าง' },
          ],
        },
        options: {
          responseMode: 'lastNode',
          inputPlaceholder: 'ถามเกี่ยวกับผล MRA แบบสรุป...',
          loadPreviousSession: 'notSupported',
          showWelcomeScreen: true,
          getStarted: 'เริ่มถามข้อมูล MRA',
          subtitle: 'ตอบจากข้อมูล Aggregate ล่าสุดที่ผ่าน Data Quality',
          title: 'MRA Quality Assistant',
          allowedOrigins: 'http://localhost:5678',
        },
      },
      id: '0b4f9fa4-97d1-4cb8-87aa-16a796c16455',
      name: 'MRA Data Chat',
      type: '@n8n/n8n-nodes-langchain.chatTrigger',
      typeVersion: 1.4,
      position: [-1080, 0],
      webhookId: 'd6d75af8-bda2-48ce-aa0f-968840d1f082',
    },
    {
      parameters: {
        operation: 'read',
        fileSelector: '/data/agent/agent_payload.json',
        options: { dataPropertyName: 'data', mimeType: 'application/json' },
      },
      id: 'f76f3a91-2bb2-42d1-a625-fe47695d1b69',
      name: 'Read Safe Agent Payload',
      type: 'n8n-nodes-base.readWriteFile',
      typeVersion: 1.1,
      position: [-840, 0],
    },
    {
      parameters: {
        operation: 'fromJson',
        binaryPropertyName: 'data',
        destinationKey: 'payload',
        options: { encoding: 'utf8', stripBOM: true },
      },
      id: '39a3a996-6176-4e48-a20a-99246c36f555',
      name: 'Extract Aggregate JSON',
      type: 'n8n-nodes-base.extractFromFile',
      typeVersion: 1.1,
      position: [-600, 0],
    },
    {
      parameters: { jsCode: guardCode },
      id: '3d4be81f-697c-450d-9d2e-979a0fb34ab9',
      name: 'Privacy Guard and Prepare Context',
      type: 'n8n-nodes-base.code',
      typeVersion: 2,
      position: [-350, 0],
    },
    {
      parameters: {
        conditions: {
          options: { caseSensitive: true, leftValue: '', typeValidation: 'strict', version: 2 },
          conditions: [{
            id: '162da7e7-b6af-471f-962b-9b078a627cd0',
            leftValue: '={{ $json.allowed }}',
            rightValue: true,
            operator: { type: 'boolean', operation: 'true', singleValue: true },
          }],
          combinator: 'and',
        },
        options: {},
      },
      id: '5d4fc5c9-1737-43de-921c-52ea97b60a63',
      name: 'Safe Aggregate Question?',
      type: 'n8n-nodes-base.if',
      typeVersion: 2.2,
      position: [-100, 0],
    },
    {
      parameters: {
        conditions: {
          options: { caseSensitive: true, leftValue: '', typeValidation: 'strict', version: 2 },
          conditions: [{
            id: '12b60343-f0e3-4e0e-93b4-00c54e121a77',
            leftValue: '={{ $json.ai_enabled }}',
            rightValue: true,
            operator: { type: 'boolean', operation: 'true', singleValue: true },
          }],
          combinator: 'and',
        },
        options: {},
      },
      id: 'b23bec7d-b57b-41f2-9974-1200d5d9eef1',
      name: 'Gemini Enabled?',
      type: 'n8n-nodes-base.if',
      typeVersion: 2.2,
      position: [150, -100],
    },
    {
      parameters: { jsCode: deterministicCode },
      id: '2b9111f8-9698-4b7f-ae92-6857c490a62c',
      name: 'Deterministic Chat Answer',
      type: 'n8n-nodes-base.code',
      typeVersion: 2,
      position: [410, 70],
    },
    {
      parameters: {
        jsCode: "const data = $input.first().json; return [{ json: { output: data.blocked_message || 'ไม่สามารถตอบคำถามนี้ได้อย่างปลอดภัยค่ะ' } }];",
      },
      id: '782052d4-e500-4f9b-8b33-ec3e62bd12e2',
      name: 'Return Privacy Message',
      type: 'n8n-nodes-base.code',
      typeVersion: 2,
      position: [150, 180],
    },
    {
      parameters: {
        promptType: 'define',
        text: '={{ $json.agent_prompt }}',
        options: {
          systemMessage: 'คุณคือ MRA Quality Assistant ตอบเฉพาะข้อมูล Aggregate ในบริบทที่ได้รับเท่านั้น ห้ามสร้างตัวเลข ห้ามขอ แสดง หรืออนุมาน HN, AN ชื่อผู้ป่วย หรือข้อมูลส่วนบุคคล หากข้อมูลไม่พอให้ตอบว่าไม่มีข้อมูล แยกข้อเท็จจริงออกจากสมมติฐานอย่างชัดเจน ตอบภาษาไทย กระชับ ตรวจสอบย้อนกลับได้ และข้อเสนอแนะต้องมี Human Review ห้ามตัดสินใจทางคลินิกอัตโนมัติ',
          maxIterations: 4,
          returnIntermediateSteps: false,
        },
      },
      id: 'd61a2962-0b30-426b-b11d-d91897fcfcf9',
      name: 'MRA Chat AI Agent',
      type: '@n8n/n8n-nodes-langchain.agent',
      typeVersion: 3.1,
      position: [410, -220],
    },
    {
      parameters: {
        modelName: 'models/gemini-3.7-flash',
        options: { maxOutputTokens: 4096 },
      },
      id: '9279c98a-7aa3-46df-b0d0-bb85c0d953e1',
      name: 'Gemini Model for Chat',
      type: '@n8n/n8n-nodes-langchain.lmChatGoogleGemini',
      typeVersion: 1.1,
      position: [410, -20],
    },
    {
      parameters: { sessionIdType: 'fromInput', contextWindowLength: 8 },
      id: '45c78740-1e12-4404-86c8-63bd403ac967',
      name: 'Chat Memory',
      type: '@n8n/n8n-nodes-langchain.memoryBufferWindow',
      typeVersion: 1.4,
      position: [600, -20],
    },
  ],
  connections: {
    'MRA Data Chat': { main: [[{ node: 'Read Safe Agent Payload', type: 'main', index: 0 }]] },
    'Read Safe Agent Payload': { main: [[{ node: 'Extract Aggregate JSON', type: 'main', index: 0 }]] },
    'Extract Aggregate JSON': { main: [[{ node: 'Privacy Guard and Prepare Context', type: 'main', index: 0 }]] },
    'Privacy Guard and Prepare Context': { main: [[{ node: 'Safe Aggregate Question?', type: 'main', index: 0 }]] },
    'Safe Aggregate Question?': {
      main: [
        [{ node: 'Gemini Enabled?', type: 'main', index: 0 }],
        [{ node: 'Return Privacy Message', type: 'main', index: 0 }],
      ],
    },
    'Gemini Enabled?': {
      main: [
        [{ node: 'MRA Chat AI Agent', type: 'main', index: 0 }],
        [{ node: 'Deterministic Chat Answer', type: 'main', index: 0 }],
      ],
    },
    'Gemini Model for Chat': { ai_languageModel: [[{ node: 'MRA Chat AI Agent', type: 'ai_languageModel', index: 0 }]] },
    'Chat Memory': { ai_memory: [[{ node: 'MRA Chat AI Agent', type: 'ai_memory', index: 0 }]] },
  },
  settings: { executionOrder: 'v1' },
  staticData: null,
  pinData: {},
  meta: { templateCredsSetupCompleted: false },
  tags: [],
};

fs.writeFileSync(
  'C:/ETL/n8n_chat_delivery/n8n/workflows/mra_data_chat.json',
  JSON.stringify(workflow, null, 2) + '\n',
  'utf8',
);
