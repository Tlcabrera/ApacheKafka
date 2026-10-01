#!/usr/bin/env bash
# Laboratorio 13 - Modelo de mínimo privilegio para tres principales.
# Ejecutar dentro del contenedor: docker exec -it broker1 bash /tmp/acls-minimo-privilegio.sh
set -euo pipefail
K=/opt/kafka/bin/kafka-acls.sh
B=${BOOTSTRAP:-broker1:19092}
CMD="--command-config /etc/kafka/secrets/admin.properties"   # credenciales del administrador (SASL)

echo "== Productor: solo escribir en su dominio =="
$K --bootstrap-server $B $CMD --add --allow-principal User:svc-facturacion-prod \
   --operation Write --operation Describe \
   --topic dian.facturacion. --resource-pattern-type prefixed

echo "== Consumidor analítico: leer el dominio Y su grupo =="
$K --bootstrap-server $B $CMD --add --allow-principal User:svc-analitica-lectura \
   --operation Read --operation Describe \
   --topic dian.facturacion. --resource-pattern-type prefixed
# Sin esta segunda regla se obtiene GroupAuthorizationException (paso 3 del laboratorio)
$K --bootstrap-server $B $CMD --add --allow-principal User:svc-analitica-lectura \
   --operation Read --group analitica- --resource-pattern-type prefixed

echo "== Cuota de consumo para proteger la plataforma compartida =="
/opt/kafka/bin/kafka-configs.sh --bootstrap-server $B $CMD --alter \
  --entity-type users --entity-name svc-analitica-lectura \
  --add-config 'consumer_byte_rate=5242880'

echo "== Verificación =="
$K --bootstrap-server $B $CMD --list --topic dian.facturacion.emitida
