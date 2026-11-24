#!/usr/bin/env bash
set -euo pipefail

DIR=${1:-./secrets}
DAYS=365
DEFAULT_PASS=${PASS:-dianlab2026}
BROKERS=(broker1)

fail() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

for command_name in openssl keytool docker; do
  command -v "$command_name" >/dev/null 2>&1 || fail "Falta el comando requerido: $command_name"
done

docker info >/dev/null 2>&1 || fail "No se puede consultar Docker para proteger volúmenes Etapa E existentes"

for logical_volume in datae; do
  while IFS= read -r existing_volume; do
    [[ -n "$existing_volume" ]] || continue
    volume_state=$(docker run --rm \
      --volume "$existing_volume:/inspect:ro" \
      --entrypoint /bin/bash apache/kafka:3.9.0 \
      -c 'if [[ -f /inspect/meta.properties ]]; then printf formatted; elif [[ -z "$(ls -A /inspect)" ]]; then printf empty; else printf nonempty; fi') \
      || fail "No pude inspeccionar el volumen $existing_volume; no genero ni roto credenciales."
    [[ "$volume_state" == empty ]] || fail "El volumen Etapa E $existing_volume contiene metadata o datos ($volume_state). No regenero credenciales; consulta al docente."
  done < <(docker volume ls -q --filter "label=com.docker.compose.volume=$logical_volume")
done

mkdir -p "$DIR"
DIR=$(cd "$DIR" && pwd)
chmod 755 "$DIR"

security_files=(
  scram-credentials.env
  broker_jaas.conf
  admin.properties
  cliente.properties
  cliente-malo.properties
  analitica.properties
)
existing_security_files=0
for file in "${security_files[@]}"; do
  [[ -e "$DIR/$file" ]] && ((existing_security_files += 1))
done
if (( existing_security_files > 0 && existing_security_files != ${#security_files[@]} )); then
  fail "Los archivos SCRAM/clientes están incompletos en $DIR; no los sobrescribo ni roto credenciales. Consulta al docente."
fi

tls_files=(ca-key ca-cert truststore.jks creds)
for broker in "${BROKERS[@]}"; do
  tls_files+=("$broker.keystore.jks")
done
existing_tls_files=0
for file in "${tls_files[@]}"; do
  [[ -e "$DIR/$file" ]] && ((existing_tls_files += 1))
done

if (( existing_tls_files == 0 )); then
  PASS_VALUE=$DEFAULT_PASS
  (
    cd "$DIR"
    printf '==> Generando CA de laboratorio y truststore\n'
    openssl req -new -x509 -keyout ca-key -out ca-cert -days "$DAYS" -nodes \
      -subj '/C=CO/ST=Bogota/L=Bogota/O=DIAN/OU=EIAN-Laboratorio/CN=ca-kafka-lab'

    keytool -keystore truststore.jks -alias CARoot -importcert -file ca-cert \
      -storepass "$PASS_VALUE" -noprompt -storetype JKS

    for broker in "${BROKERS[@]}"; do
      printf '==> Generando keystore para %s\n' "$broker"
      keytool -keystore "$broker.keystore.jks" -alias "$broker" -validity "$DAYS" \
        -genkeypair -keyalg RSA -storepass "$PASS_VALUE" -keypass "$PASS_VALUE" \
        -storetype JKS \
        -dname "CN=$broker,OU=EIAN,O=DIAN,L=Bogota,ST=Bogota,C=CO" \
        -ext "SAN=DNS:$broker,DNS:localhost,IP:127.0.0.1"

      keytool -keystore "$broker.keystore.jks" -alias "$broker" -certreq \
        -file "$broker.csr" -storepass "$PASS_VALUE"
      openssl x509 -req -CA ca-cert -CAkey ca-key -in "$broker.csr" \
        -out "$broker-signed.crt" -days "$DAYS" -CAcreateserial \
        -extfile <(printf 'subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\n' "$broker")
      keytool -keystore "$broker.keystore.jks" -alias CARoot -importcert \
        -file ca-cert -storepass "$PASS_VALUE" -noprompt
      keytool -keystore "$broker.keystore.jks" -alias "$broker" -importcert \
        -file "$broker-signed.crt" -storepass "$PASS_VALUE" -noprompt
    done

    printf '%s\n' "$PASS_VALUE" > creds
  )
elif (( existing_tls_files != ${#tls_files[@]} )); then
  fail "TLS material is incomplete in $DIR. No files were overwritten; ask the instructor to inspect it."
else
  PASS_VALUE=$(<"$DIR/creds")
  [[ -n "$PASS_VALUE" ]] || fail "The keystore password file $DIR/creds is empty"
  printf '==> Reusing existing TLS certificates and keystores\n'
fi

if (( existing_security_files == 0 )); then
  ADMIN_PASSWORD=$(openssl rand -hex 24)
  FACTURACION_PASSWORD=$(openssl rand -hex 24)
  ANALITICA_PASSWORD=$(openssl rand -hex 24)
  BAD_PASSWORD=$(openssl rand -hex 24)

  cat > "$DIR/scram-credentials.env" <<EOF
SCRAM_ADMIN_KAFKA='SCRAM-SHA-512=[name=admin-kafka,password=$ADMIN_PASSWORD]'
SCRAM_SVC_FACTURACION_PROD='SCRAM-SHA-512=[name=svc-facturacion-prod,password=$FACTURACION_PASSWORD]'
SCRAM_SVC_ANALITICA_LECTURA='SCRAM-SHA-512=[name=svc-analitica-lectura,password=$ANALITICA_PASSWORD]'
EOF

  cat > "$DIR/broker_jaas.conf" <<EOF
KafkaServer {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="admin-kafka"
  password="$ADMIN_PASSWORD";
};
EOF

write_client_config() {
  local file=$1
  local username=$2
  local password=$3
  cat > "$DIR/$file" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";
ssl.truststore.location=/etc/kafka/secrets/truststore.jks
ssl.truststore.password=$PASS_VALUE
ssl.truststore.type=JKS
EOF
}

  write_client_config admin.properties admin-kafka "$ADMIN_PASSWORD"
  write_client_config cliente.properties svc-facturacion-prod "$FACTURACION_PASSWORD"
  write_client_config analitica.properties svc-analitica-lectura "$ANALITICA_PASSWORD"
  write_client_config cliente-malo.properties svc-facturacion-prod "$BAD_PASSWORD"
else
  for file in "${security_files[@]}"; do
    [[ -s "$DIR/$file" ]] || fail "El archivo $DIR/$file existe pero está vacío. No sobrescribo credenciales."
  done
  printf '==> Reusing complete existing SCRAM credentials and client profiles\n'
fi

chmod 600 "$DIR/ca-key"
chmod 644 "$DIR"/scram-credentials.env "$DIR"/broker_jaas.conf \
  "$DIR"/*.properties "$DIR"/creds "$DIR"/ca-cert "$DIR"/truststore.jks \
  "$DIR"/*.keystore.jks

printf '\nMateriales listos en %s (no compartas los secretos).\n' "$DIR"
printf 'Archivos: ca-cert, creds, truststore.jks, broker1.keystore.jks, broker_jaas.conf, scram-credentials.env y perfiles .properties.\n'
printf 'Vigencia certificados: %s días. Uso exclusivo de laboratorio.\n' "$DAYS"
