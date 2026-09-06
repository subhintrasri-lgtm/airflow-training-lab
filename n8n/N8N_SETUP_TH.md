# เชื่อม Airflow กับ n8n

## ที่อยู่ระบบ

- Airflow: http://localhost:8081
- n8n: http://localhost:5678
- Webhook ภายใน Docker: `http://n8n:5678/webhook/mra-agentic-quality`

## Flow

`Airflow build_agent_payload` → `Airflow notify_n8n` → `n8n Webhook` → `Validate Agent Payload` → `Quality Gate` → `AI Agent/Fallback` → `HTTP Response`

ภายใน Workflow เดียวกันมีจุดเริ่มต้น 2 ทาง:

1. `Airflow Webhook` → `Validate` → `Quality Gate` → `AI Agent/Fallback` → `HTTP Response`
2. `n8n Chat` → `อ่าน agent_payload.json` → `Privacy Guard` → `Gemini AI/คำตอบสำรอง` → `ตอบภาษาไทย`

## โครงสร้าง Node หลักตามโจทย์

- `Chat Trigger` รับคำถามจากผู้ใช้
- `AI Agent` วิเคราะห์คำถามและเลือกใช้ Tool
- `Conversation Memory` จำบริบทการสนทนา 8 ข้อความ
- `Google Gemini Model` เป็น Language Model ของ Agent
- `Get_Airflow_Log` เป็น HTTP Request Tool แบบ GET และต่อเข้าช่อง `ai_tool` ของ Agent

`Get_Airflow_Log` ใช้ URL ฐาน `http://airflow-webserver:8080/api/v1/{api_path}` และใช้ Credential `Airflow API Basic Auth` ซึ่ง n8n เก็บแบบเข้ารหัส ไม่เก็บ username/password ใน Workflow JSON หรือ GitHub

เมื่อต้องอ่าน Log ล่าสุด Agent จะใช้ Tool ตามลำดับ: หา DAG run ล่าสุด → อ่าน task instances → อ่าน Log ของ task ที่เกี่ยวข้อง

> โปรเจกต์นี้ใช้ AI Agent `2.2` เฉพาะเส้นทาง Chat เพราะเข้ากันได้กับ HTTP Request Tool ใน n8n `2.37.10` ที่ติดตั้งอยู่ ส่วนชื่อและหน้าที่ของ Node ยังตรงตามโจทย์เดิม

## ใช้งาน Chat

1. รัน DAG `mra_agentic_pipeline` อย่างน้อย 1 ครั้ง เพื่อสร้างข้อมูลสรุปล่าสุด
2. เปิด n8n ที่ http://localhost:5678
3. เปิด Workflow `MRA - Airflow Quality + AI Chat`
4. กดปุ่ม **Open chat** ที่มุมล่างของหน้า Workflow
5. ทดลองถาม เช่น `สรุปสถานะคุณภาพข้อมูลล่าสุด` หรือ `จุดที่ completion rate ต่ำที่สุดคืออะไร`

หรือเปิดหน้า Chat โดยตรงที่:

http://localhost:5678/webhook/a74231de-4bf9-438a-baad-ea97cb3f7d74/chat

Chat อ่านเฉพาะ `data/agent/agent_payload.json` ซึ่งเป็นข้อมูล Aggregate ไม่มี HN, AN หรือชื่อผู้ป่วย หากถามข้อมูลรายบุคคล ระบบจะปฏิเสธอัตโนมัติ

เส้นทาง Chat เปิดใช้ Gemini แล้วและต้องมี Credential `Google Gemini(PaLM) Api account` ใน n8n หาก Gemini ใช้งานไม่ได้ ระบบจะส่งข้อความแจ้งข้อผิดพลาดแทนการสร้างคำตอบขึ้นเอง

## การวิเคราะห์ผล

- ค่าเริ่มต้น `N8N_AI_ENABLED=false`: วิเคราะห์และเสนอแนะด้วยกฎ deterministic โดยไม่เสียค่า API
- เมื่อเปิด `N8N_AI_ENABLED=true`: ใช้ Gemini AI Agent วิเคราะห์ข้อมูล aggregate และเสนอแผน 7/30/90 วัน
- ทั้งสองทางกำหนด `human_review_required=true`
- ข้อมูลที่ส่งไม่มี HN, AN หรือข้อมูลระบุตัวผู้ป่วย

## เปิดใช้ Gemini AI Agent

1. สร้าง API Key ที่ Google AI Studio
2. เปิด n8n ที่ http://localhost:5678
3. ไปที่ **Credentials** → **Create Credential**
4. เลือก **Google Gemini(PaLM) API** แล้ววาง API Key และบันทึก
5. เปิด Workflow `MRA - Airflow Quality + AI Chat`
6. เปิดกล่อง `Google Gemini Chat Model` แล้วเลือก Credential ที่สร้างไว้
7. ใน Workflow เดียวกัน เลือก Credential เดียวกันในกล่อง `Google Gemini Model`
8. เปิดไฟล์ `.env` ของโปรเจกต์และเพิ่ม `N8N_AI_ENABLED=true` เมื่อต้องการให้ Airflow เรียก AI วิเคราะห์อัตโนมัติด้วย
9. รีเฟรช Airflow Worker:

```powershell
docker compose up -d --force-recreate airflow-worker
```

10. กด Save/Publish Workflow เดียว และรัน DAG `mra_agentic_pipeline` ใหม่

ห้ามบันทึก Gemini API Key ลงใน Workflow JSON, `.env.example` หรือ GitHub

## เปิดระบบ

วิธีง่ายที่สุด ให้เปิด PowerShell ในโฟลเดอร์โปรเจกต์แล้วรัน:

```powershell
powershell -ExecutionPolicy Bypass -File .\n8n\setup-n8n.ps1
```

สคริปต์จะเปิด n8n, นำเข้า Workflow, Publish Production Webhook และรีสตาร์ต n8n ให้อัตโนมัติ

ครั้งแรกให้เปิด http://localhost:5678 และสร้างบัญชี Owner ในเครื่องนี้ จากนั้น Workflow จะปรากฏในหน้า Workflows

หากต้องการทำทีละคำสั่ง:

```powershell
docker compose up -d n8n
docker compose exec -T n8n n8n import:workflow --input=/workflows/mra_airflow_webhook.json
docker compose exec -T n8n n8n publish:workflow --id=MraAirflowWebhook01
docker compose restart n8n
```

## ทดสอบ

รัน DAG `mra_agentic_pipeline` จาก Airflow แล้วเปิดไฟล์:

`data/output/mra_agentic/n8n_delivery_receipt.json`

ผลสำเร็จต้องมี `status` เป็น `SENT`, HTTP 200 และคำตอบจาก n8n เป็น `ANALYZED`

- `analysis_mode=DETERMINISTIC_FALLBACK`: ยังไม่ได้เปิด Gemini
- `analysis_mode=GEMINI_AI_AGENT`: Gemini วิเคราะห์สำเร็จ

ทดสอบ Chat และ Airflow Log Tool ด้วยคำถาม:

`ช่วยตรวจสอบ Log ล่าสุดของ Airflow สำหรับ DAG mra_agentic_pipeline และสรุปว่ารันสำเร็จหรือมี error`

Agent ต้องเรียก `Get_Airflow_Log` สามครั้งตามลำดับ และตอบด้วย `dag_run_id`, สถานะ DAG run, สถานะ Task และข้อจำกัด/Human Review โดยห้ามแสดง Credential หรือข้อมูลผู้ป่วย

> การตั้งค่านี้จำกัดหน้า n8n ไว้ที่ localhost เหมาะสำหรับการเรียนและทดสอบในเครื่อง หากนำขึ้น Server จริง ต้องเพิ่ม HTTPS และ Webhook authentication
