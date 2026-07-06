FROM apache/airflow:2.9.3

USER root
# air4thai.pcd.go.th ใช้ Let's Encrypt intermediate/root ("YR1" / "ISRG Root YR")
# ที่ออกใหม่ (ก.ย. 2025) ยังไม่อยู่ใน ca-certificates package ของ Debian bookworm ตอนนี้
# ถ้าไม่เพิ่มเอง requests/urllib3 (และ curl/openssl) จะ verify SSL fail กับโดเมนนี้
RUN curl -sS -m 15 http://yr1.i.lencr.org/ -o /tmp/yr1.der \
    && curl -sS -m 15 http://yr.i.lencr.org/ -o /tmp/root_yr.der \
    && openssl x509 -inform DER -in /tmp/yr1.der -out /usr/local/share/ca-certificates/letsencrypt-yr1.crt \
    && openssl x509 -inform DER -in /tmp/root_yr.der -out /usr/local/share/ca-certificates/isrg-root-yr.crt \
    && rm -f /tmp/yr1.der /tmp/root_yr.der \
    && update-ca-certificates
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
