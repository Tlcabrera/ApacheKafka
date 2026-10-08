# Laboratorio 12 — Habilitar TLS y SASL/SCRAM
**Sesión 16** · Módulo 7.1 · 40 min guiados en vivo · Informe (martes 17/11)

> **Entorno aislado.** Este laboratorio usa exclusivamente `docker-compose.etapaE.yml`. No lo combine con `docker-compose-lab-kafka.yml`: las etapas tienen volúmenes, listeners y credenciales distintos.
> En AWS/code-server, esta etapa cambia el clúster compartido completo. Debe ejecutarla el docente o un equipo asignado mientras los demás observan y toman evidencias.

## Objetivo

Generar una CA de laboratorio y certificados, arrancar un clúster KRaft con TLS y SASL/SCRAM, comprobar autenticación correcta e incorrecta, y verificar el handshake TLS. Los datos siguen siendo sintéticos; las claves del ejercicio no son un patrón de producción.

## Paso 1 — Generar certificados y configuraciones de cliente ★

```bash
cd ~/kafka-dian/entorno
bash gen-certs.sh ./secrets
bash gen-client-configs.sh ./secrets
ls secrets/
keytool -list -keystore secrets/broker1.keystore.jks -storepass dianlab2026 | head
```

**Salida esperada:** CA, truststore, un keystore por broker, `broker_jaas.conf`, `scram-credentials.env` y cuatro archivos `.properties`. `scram-credentials.env` contiene las mismas credenciales iniciales para los tres nodos; no lo edite ni lo distribuya.

## Paso 2 — Arrancar la etapa E ★

```bash
# La etapa E reutiliza los nombres broker1, broker2 y broker3. Detiene y elimina
# solamente los contenedores de la etapa base; no borra volúmenes porque no usa -v.
docker compose -f docker-compose-lab-kafka.yml down
docker compose -f docker-compose.etapaE.yml up -d
docker compose -f docker-compose.etapaE.yml ps
docker compose -f docker-compose.etapaE.yml logs broker1 | grep -E "started|ERROR|SASL" | tail -20
```

**Salida esperada:** los tres brokers quedan `healthy`. `down` sin `-v` conserva los
volúmenes de la etapa base. Si ya había una prueba anterior de la etapa E con
configuración distinta, deténgase y consulte al docente antes de ejecutar
`docker compose -f docker-compose.etapaE.yml down -v`: ese comando elimina los
volúmenes de la práctica.

## Paso 3 — Verificar SCRAM y TLS ★

```bash
docker exec broker1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker1:19092 \
  --command-config /etc/kafka/secrets/admin.properties --describe --entity-type users

openssl s_client -connect localhost:9093 -servername broker1 </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -issuer
```

**Observe:** aparecen los tres principales SCRAM y el certificado presentado por `broker1`. El listener interno también usa `SASL_SSL`; por eso el comando administrativo lleva `admin.properties`.

## Paso 4 — Cliente autenticado ★

```bash
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 \
  --command-config /etc/kafka/secrets/admin.properties --list
```

**Salida esperada:** el administrador autentica y puede listar. El principal `svc-facturacion-prod` todavía no puede escribir porque la autorización por mínimo privilegio se configura en el Lab 13.

## Paso 5 — Accesos denegados ★

```bash
# a) sin credenciales
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 --list

# b) con contraseña incorrecta
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 \
  --command-config /etc/kafka/secrets/cliente-malo.properties --list

docker compose -f docker-compose.etapaE.yml logs broker1 | grep -i "authenticat" | tail -10
```

**Salida esperada:** el primer comando no logra negociar con el listener seguro; el segundo informa `SaslAuthenticationException` o un fallo de autenticación equivalente. Registre el mensaje del cliente y la línea del broker.

## Paso 6 — Evidencia de cifrado

Conserve la salida del handshake TLS del paso 3 y explique qué valida: servidor, cadena de confianza y nombre del host. La comparación de paquetes en claro queda fuera del ejercicio obligatorio porque requeriría un listener PLAINTEXT temporal y una imagen de diagnóstico separada; no se habilita un puerto en claro dentro de esta etapa segura.

## Informe

Incluya las salidas de los pasos ★ y responda: (a) ¿por qué las credenciales de comunicación interna se crean al formatear KRaft?; (b) diseñe la rotación de certificados para la entidad; (c) ¿cuándo elegiría SCRAM y cuándo Kerberos?; (d) ¿qué riesgo evita que TLS cubra también el listener interno?
