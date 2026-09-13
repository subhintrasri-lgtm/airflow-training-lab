# airflow-training-lab
สำหรับการฝึกอบรมหรือทดลองใช้งาน Apache Airflow แบบ Online โดยไม่ต้องติดตั้งลงเครื่องคอมพิวเตอร์ของผู้เรียน

สำหรับผู้เรียน (วิธีเข้าใช้งานผ่าน Browser)**

1. **Fork Repository ไปยังบัญชีของตนเอง:**
1. ผู้เรียนเปิดลิงก์ Repository ของผู้สอน https://github.com/anantchok/airflow-training-lab
2. กดปุ่ม **Fork** ด้านขวาบน เพื่อคัดลอกโปรเจกต์ไปยังบัญชี GitHub ของตนเอง


2. **เปิดใช้งาน GitHub Codespaces:**
1. ในหน้า Repo ที่ Fork มา ให้กดปุ่มสีเขียว **`<> Code`**
2. เลือกแท็บ **Codespaces**
3. กดปุ่ม **Create codespace on main** (ระบบจะเปิดหน้าจอ VS Code บนเบราว์เซอร์ ใช้เวลาเตรียมเครื่องประมาณ 1–2 นาที)


3. **รันคำสั่งเริ่มต้นระบบ Airflow:**
เมื่อหน้าต่าง Terminal ด้านล่างปรากฏขึ้น ให้พิมพ์คำสั่งตามลำดับ:

```bash
# 1. Initial ฐานข้อมูลและสร้าง User เริ่มต้น
docker compose up airflow-init

# 2. เมื่อขึ้นสถานะ airflow-init completed แล้ว ให้สั่งรันทั้งระบบแบบ Background
docker compose up -d

```

## การอ่านไฟล์ MRA จาก OneDrive อัตโนมัติ

กำหนด `MRA_ONEDRIVE_HOST_DIR` ในไฟล์ `.env` ให้ชี้ไปยังโฟลเดอร์
OneDrive ที่มีไฟล์ Audit ทั้ง 8 ไฟล์ แล้วสร้าง Airflow containers ใหม่ด้วย
`docker compose up -d --force-recreate airflow-webserver airflow-scheduler airflow-worker airflow-triggerer`

โฟลเดอร์ OneDrive ถูก mount เข้า Airflow แบบ read-only เท่านั้น Task
`sync_onedrive_inputs` จะคัดลอกทั้ง 8 ไฟล์ไปยัง snapshot ภายใน
`data/staging/mra_agentic/source_snapshot` และตรวจ SHA-256 ก่อนเริ่ม Extract
หากไฟล์ขาดหรือ OneDrive เปลี่ยนไฟล์ระหว่างคัดลอก Task จะหยุดโดยไม่ใช้ snapshot
ที่ไม่สมบูรณ์

ไฟล์ `ResultMRauditJCIHA.xlsx` ยังคงอ่านจาก `data/input` และข้อมูลต้นฉบับ,
staging, output รวมถึงไฟล์ `.env` ต้องไม่ถูก commit ขึ้น GitHub

### Mapping กับไฟล์ MOI Report

กำหนด `MRA_MOI_ONEDRIVE_HOST_DIR` ใน `.env` ให้ชี้ไปยังโฟลเดอร์ที่มีไฟล์ต่อไปนี้:

- `NewAuditForm2025.xlsx` ใช้ Mapping รหัสคำถามด้วย `SProID`
- `MRCode.xlsx` ใช้ Mapping `ClusterID`, `DischargeWardName`, `MainICD` และตรวจการมีอยู่ของ `DoctorCode`
- `SubClusterID.xlsb` ใช้ Mapping `ClusterID` และกำหนดชื่อ Cluster จาก `SCShortName`
- `HAReportJan2024-May2025.xlsm` ใช้ตรวจเทียบจำนวน encounter ของ IPD (`HN+AN`) และ OPD (`HN+VisitDate`) เฉพาะช่วงเวลาเดียวกัน

Airflow mount โฟลเดอร์นี้แบบ read-only และ Task `sync_moi_reference_inputs`
จะสร้าง snapshot พร้อมตรวจ SHA-256 ก่อน Mapping ทุกครั้ง จากนั้น Task
`build_moi_reference_dimensions`, `map_moi_references` และ `reconcile_ha_report`
จะตรวจคีย์ซ้ำและบันทึก mapping coverage ใน Quality Report

MariaDB จะได้รับตาราง snapshot เพิ่มเติม ได้แก่ `mra_question_dimension`,
`mra_cluster_dimension`, `mra_ward_dimension`, `mra_icd10_dimension` และ
`mra_doctor_code_dimension` ส่วน n8n/AI จะได้รับเฉพาะจำนวนและอัตราสรุป
โดยไม่ส่ง HN, AN, ชื่อแพทย์ หรือ encounter key เข้า LLM

ตาราง `mra_compliance_by_cluster` เก็บ Compliance ราย Cluster-รายเดือน และ View
`mra_compliance_by_cluster_overall` รวมผลตาม Cluster ชื่อในผลลัพธ์ใช้ `SCShortName`
จาก `SubClusterID.xlsb` การคำนวณใช้ `SUM(Value) / COUNTROWS(MRA Data)` เช่นเดียวกับ
Power BI ส่วน n8n ได้รับ `compliance_by_cluster` และจุดคะแนนต่ำสูงสุด 10 รายการต่อ
Cluster ใน `cluster_improvement_areas` เพื่อใช้ตอบคำถามด้านการปรับปรุงแบบ Aggregate

### Power BI และ Compliance rate

Power BI และ n8n ใช้นิยามเดียวกันคือ
`Compliance rate = SUM(Value) / COUNTROWS(MRA Data)` โดยห้ามนำ rate ของแต่ละกลุ่ม
มาเฉลี่ยตรง ๆ ตาราง `mra_compliance_by_profession` เก็บผลรายวิชาชีพ-รายเดือน
และ View `mra_compliance_by_profession_overall` รวมผลตามวิชาชีพสำหรับ Power BI

Power BI Desktop บนเครื่อง Host เชื่อม MariaDB ที่ `localhost:3308` และเลือกตาราง/View
ดังกล่าวได้ ส่วน n8n อ่านข้อมูลเดียวกันจาก `agent_payload.json` ในหัวข้อ
`compliance_by_profession` และ `compliance_by_profession_period` โดยไม่มีข้อมูลผู้ป่วย

Airflow สร้างกราฟแท่งแนวนอนจากข้อมูล Aggregate เดียวกันไว้ที่
`data/agent/compliance_by_profession.svg` และ n8n ให้บริการกราฟภายในเครื่องที่
`http://localhost:5678/webhook/mra-compliance-chart` เมื่อผู้ใช้ถามหา “กราฟ” หรือ
“chart” คำตอบจะแสดงทั้งกราฟ ลิงก์เปิดภาพขนาดเต็ม และข้อความสรุปตัวเลข กราฟนี้ไม่ใช้
บริการภายนอกและไม่มีข้อมูลระบุตัวผู้ป่วย

กราฟราย Cluster อยู่ที่ `data/agent/compliance_by_cluster.svg` และเปิดผ่าน
`http://localhost:5678/webhook/mra-cluster-compliance-chart` ภายใน n8n Workflow
เดียวกัน ชื่อ Cluster บนกราฟใช้ `SCShortName` และแสดง `ClusterID` กำกับ


4. **เปิด Airflow Web UI:**
1. รอประมาณ 30–60 วินาที จะมี Popup แจ้งเตือนพอร์ต **8080** เด้งขึ้นมามุมขวาล่าง ให้คลิก **Open in Browser** (หรือไปที่แท็บ **PORTS** ด้านล่าง แล้วคลิกไอคอนลูกโลกที่ Port `8080`)
2. หากขึ้น error ให้รอระบบสำหรับการเปิดใช้งานประมาณ 1-2 นาทีเนื่องจากต้องใช้เวลาในช่วงเริ่มใช้งานครั้งแรก
3. ระบบจะเปิดแท็บใหม่เข้าสู่หน้า Airflow Login
4. กรอก Username: `airflow` และ Password: `airflow` เพื่อเริ่มใช้งานและแก้ไขโค้ดในโฟลเดอร์ `dags/` ได้ทันที
5. ระบบจะไปที่หน้า home หาก url เป็น: localhost/home ให้ click back url ที่ใช้งานจริง 

---

เมื่อผู้สอนมีการอัปเดตโค้ดที่ Repository ต้นทาง (Upstream) นักเรียนสามารถดึงส่วนที่เปลี่ยนแปลงมาอัปเดตลงใน Fork ของตนเองและ Codespaces ได้ วิธีหลักดังนี้:

**วิธีที่ซิงค์ผ่านหน้าเว็บ GitHub (วิธีที่ง่ายที่สุด)**

1. **กด Sync Fork บนหน้า GitHub:**
1. ให้นักเรียนเปิดหน้า Repository ของตนเองบน GitHub
2. ด้านล่างชื่อ Repository จะมีแถบแจ้งเตือนสถานะ ให้คลิกปุ่ม **Sync fork**
3. เลือก **Update branch** (โค้ดใน Branch `main` บน GitHub ของนักเรียนจะอัปเดตตามผู้สอนทันที)


2. **ดึงโค้ดเข้าสู่ Codespaces:**
กลับไปที่หน้าต่าง **GitHub Codespaces** แล้วเปิด Terminal พิมพ์คำสั่ง:

```bash
git pull origin main

```

โค้ดในเครื่อง Codespaces จะอัปเดตเป็นเวอร์ชันล่าสุดทันที


**ข้อควรระวังหลังการ Sync:**

* **ไฟล์ DAG:** Airflow จะตรวจจับไฟล์ในโฟลเดอร์ `dags/` อัตโนมัติภายใน 30–60 วินาที โดยไม่ต้องรีสตาร์ตระบบ
* **ไฟล์ docker-compose.yaml หรือ .env:** หากผู้สอนแก้ไขคอนฟิก ให้นักเรียนรันคำสั่งนี้ใน Terminal เพื่ออัปเดตคอนเทนเนอร์:

```bash
docker compose up -d

```
