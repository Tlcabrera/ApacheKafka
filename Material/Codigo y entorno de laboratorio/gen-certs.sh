#!/usr/bin/env bash
# Laboratorio 12 - Genera la autoridad certificadora de laboratorio, los keystores de los
# tres brokers y el truststore común. SOLO PARA FORMACIÓN: en producción los certificados
# los emite la PKI institucional.
set -euo pipefail
DIR=${1:-./secrets}; PASS=${PASS:-dianlab2026}; DIAS=365
mkdir -p "$DIR"; cd "$DIR"

echo "==> 1. Autoridad certificadora de laboratorio"
openssl req -new -x509 -keyout ca-key -out ca-cert -days $DIAS -nodes \
  -subj "/C=CO/ST=Bogota/L=Bogota/O=DIAN/OU=EIAN-Laboratorio/CN=ca-kafka-lab"

echo "==> 2. Truststore común (confía en la CA)"
keytool -keystore truststore.jks -alias CARoot -import -file ca-cert \
  -storepass "$PASS" -noprompt -storetype JKS

for BROKER in broker1 broker2 broker3; do
  echo "==> 3. Keystore de $BROKER"
  keytool -keystore "$BROKER.keystore.jks" -alias "$BROKER" -validity $DIAS -genkey -keyalg RSA \
    -storepass "$PASS" -keypass "$PASS" -storetype JKS \
    -dname "CN=$BROKER,OU=EIAN,O=DIAN,L=Bogota,ST=Bogota,C=CO" \
    -ext "SAN=DNS:$BROKER,DNS:localhost,IP:127.0.0.1"      # el SAN evita el fallo de verificación de nombre

  keytool -keystore "$BROKER.keystore.jks" -alias "$BROKER" -certreq -file "$BROKER.csr" -storepass "$PASS"
  openssl x509 -req -CA ca-cert -CAkey ca-key -in "$BROKER.csr" -out "$BROKER-signed.crt" \
    -days $DIAS -CAcreateserial -extfile <(printf "subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1" "$BROKER")
  keytool -keystore "$BROKER.keystore.jks" -alias CARoot -import -file ca-cert -storepass "$PASS" -noprompt
  keytool -keystore "$BROKER.keystore.jks" -alias "$BROKER" -import -file "$BROKER-signed.crt" -storepass "$PASS" -noprompt
done

echo "$PASS" > creds
echo "==> Listo. Archivos en $(pwd):"; ls -1
echo "Vigencia: $DIAS días. Anote la fecha de vencimiento: $(date -d "+$DIAS days" +%Y-%m-%d 2>/dev/null || date -v+${DIAS}d +%Y-%m-%d)"
