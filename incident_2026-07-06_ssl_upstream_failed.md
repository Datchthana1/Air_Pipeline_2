# Incident: `PL0_ingestion_air_station` upstream_failed (SSL certificate verify failed)

## อาการที่เจอ
- DAG `PL0_ingestion_air_station` fail ทุกรอบ (ทุกชั่วโมง) ตั้งแต่ **2026-07-02 02:45 UTC**
- Task `ingest` ขึ้น **failed**
- Task `trigger_transform_pipeline` ขึ้น **upstream_failed** ตามมา (เพราะ upstream คือ `ingest` fail ก่อน)

## Error ที่เจอใน log
```
requests.exceptions.SSLError: HTTPSConnectionPool(host='air4thai.pcd.go.th', port=443):
Max retries exceeded with url: /services/getNewAQI_JSON.php
(Caused by SSLError(SSLCertVerificationError(1,
'[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate')))
```
เกิดที่ `dags/function/ingest.py` → `fetch_air4thai()` → `requests.get(air4thai_url)`

## Root Cause (สาเหตุจริง — มี 2 ชั้นซ้อนกัน)

### ชั้นที่ 1: `air4thai.pcd.go.th` เปลี่ยนใบรับรอง SSL เมื่อ 2026-07-02
เว็บเปลี่ยนไปใช้ certificate ที่ออกโดย Let's Encrypt intermediate ตัวใหม่ชื่อ **`YR1`** (ออกให้ตั้งแต่ Sep 2025) แทนที่ chain เดิม และเซิร์ฟเวอร์ **ส่ง certificate chain ไม่ครบ/ผิด** — ไม่ส่ง intermediate `YR1` มาด้วย (นี่คือ**ความผิดพลาดฝั่งเซิร์ฟเวอร์ของ air4thai เอง** ไม่ใช่ปัญหาที่เราแก้ได้จากฝั่ง client โดยตรง)

### ชั้นที่ 2: root CA ของ intermediate ตัวนี้ยังไม่อยู่ใน Debian `ca-certificates`
`YR1` เซ็นโดย root ตัวใหม่ล่าสุดของ Let's Encrypt ชื่อ **`ISRG Root YR`** (ใช้งานตั้งแต่ Sep 2025) ซึ่ง**ยังไม่ถูกรวมเข้า `ca-certificates` package ของ Debian bookworm** ที่ image `apache/airflow:2.9.3` ใช้อยู่ — ทำให้ container ไม่มีทางเชื่อถือ chain นี้ได้เลยแม้เซิร์ฟเวอร์จะส่ง certificate มาครบ

### ทำไม Windows (host) เชื่อมต่อได้ปกติ แต่ container พังอย่างเดียว
Windows มีฟีเจอร์ **AIA Chasing** — เมื่อเจอ certificate ที่ขาด intermediate จะไปดึงมาจาก URL ที่ระบุไว้ในตัว certificate เองอัตโนมัติ (`Authority Information Access`) แล้ว cache ไว้ใช้ ทำให้ดูเหมือน "เชื่อมต่อได้ปกติ" ทั้งที่จริงๆ เซิร์ฟเวอร์ส่ง chain มาไม่ครบเหมือนกัน — แต่ Linux/OpenSSL/Python (`requests`) **ไม่ทำ AIA chasing ให้อัตโนมัติ** เลย fail ตรงๆ

### ทำไม `curl` ผ่านแต่ Python `requests` ยัง fail (หลังแก้ชั้นแรก)
Python `requests`/`urllib3` ใช้ CA bundle จาก package **`certifi`** ของตัวเอง (แยกจาก system CA store ที่ `curl`/`openssl` ใช้) — อัปเดต system CA store แล้วไม่มีผลกับ `requests` โดยตรง ต้องบังคับผ่าน env var `REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE` ให้ชี้ไปที่ system bundle แทน

## การแก้ไข

1. ดึง certificate ที่ขาดหายมาติดตั้งเป็น trusted CA ใน container:
   - `letsencrypt-yr1.crt` (จาก `http://yr1.i.lencr.org/`)
   - `isrg-root-yr.crt` (จาก `http://yr.i.lencr.org/`)
   - รัน `update-ca-certificates` เพื่อ rebuild trust store
2. ตั้ง `REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE` ให้ `requests` ใช้ system CA bundle แทน `certifi`
3. ทดสอบ `airflow tasks test PL0_ingestion_air_station ingest` → สำเร็จ (ingest 174 rows, push เข้า Supabase ได้ปกติ)

## ทำให้ถาวร (กันปัญหาเกิดซ้ำหลัง rebuild/restart)

- **`Dockerfile`**: เพิ่มขั้นตอนดึง 2 certificate นี้มาติดตั้งตอน build image (ด้วย `USER root` ... `update-ca-certificates`)
- **`docker-compose.yml`**: เพิ่ม `REQUESTS_CA_BUNDLE` และ `SSL_CERT_FILE` ใน `environment:` ของ service `airflow`

**สถานะ: แก้เสร็จสมบูรณ์แล้ว** — รัน `docker compose up -d --build` เพื่อ rebuild image + recreate container ด้วย env var ใหม่แล้ว จากนั้นทดสอบ `airflow tasks test PL0_ingestion_air_station ingest` บน container ใหม่ (ไม่ต้องใส่ env var มือเอง) → **สำเร็จ** (ingest 174 rows, push เข้า Supabase ได้ปกติ) ยืนยันว่าการแก้ไขติดถาวรใน image/config แล้ว รอบ schedule ถัดไปของ DAG ควรจะผ่านสีเขียวตามปกติ

## บทเรียน / สิ่งที่ควรจับตาต่อ
- ปัญหานี้เป็นความผิดของฝั่งเซิร์ฟเวอร์ `air4thai.pcd.go.th` (ส่ง chain ไม่ครบ) ผสมกับ root CA ใหม่ที่ distro ยังตามไม่ทัน — ถ้า Let's Encrypt หมุนเวียน root/intermediate อีกในอนาคต อาจเจอปัญหาแบบเดียวกันซ้ำได้
- ควรพิจารณา pin base image ให้ rebuild เป็นระยะ (หรือรัน `apt-get update && apt-get upgrade ca-certificates` เป็น routine) เพื่อให้ trust store ทันสมัยอยู่เสมอ
- ถ้าอยากให้ Airflow แจ้งเตือนเร็วกว่านี้ (ไม่ใช่มาเจอเองหลัง 4 วัน) ควรตั้ง alert บน task failure (เช่น `on_failure_callback` ส่งเข้า Slack/Line) แทนการเข้ามาเช็ค UI เอง
