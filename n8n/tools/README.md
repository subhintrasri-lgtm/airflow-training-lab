# n8n build & inspection tools

สคริปต์ที่ใช้สร้างและรวม workflow ของโครงงาน MRA ย้ายมาจาก `C:\ETL\n8n_*_delivery\`
เมื่อ 7 ก.ย. 2026 เพื่อรวมประวัติ n8n ไว้ในที่เดียว ทั้งหมดเป็นเครื่องมือ dev
ไม่ได้ถูกเรียกใช้ตอน runtime และไม่มี credential ฝังอยู่

| ไฟล์ | หน้าที่ |
|---|---|
| `build_mra_chat_workflow.js` | สร้าง workflow `MRA - Chat with Quality Data` (11 nodes) ตั้งแต่ต้น |
| `merge_workflows.js` | รวม chat workflow เข้ากับ webhook workflow เป็นตัวเดียว 23 nodes |
| `add_airflow_http_tool.js` | เพิ่ม tool `Get_Airflow_Log` (HTTP request tool) ให้ AI Agent |
| `inspect_n8n_workflow_refs.js` | ตรวจว่า node อ้าง workflow/credential อะไรบ้าง |
| `inspect_n8n_credentials.js` | อ่านรายชื่อ credential จาก `database.sqlite` ของ n8n (read-only) |
| `inspect_n8n_executions.py` | อ่าน execution ล่าสุดจาก snapshot ของ `database.sqlite` เพื่อ debug tool call |

`inspect_n8n_executions.py` ต้องมี `database_snapshot.sqlite` วางไว้ข้าง ๆ ก่อนรัน
และจะเขียน `latest_execution_data.txt` ออกมา — อย่า commit ไฟล์ทั้งสองนั้น

## workflows/

- `mra_airflow_webhook.json` — ตัวที่ใช้จริง 23 nodes (webhook + AI chat รวมกันแล้ว)
- `mra_data_chat.json` — chat workflow เดี่ยว 11 nodes ก่อน merge เก็บไว้อ้างอิง

## archive/

- `mra_airflow_webhook_before_ai.json` — webhook workflow ก่อนเพิ่มส่วน AI
