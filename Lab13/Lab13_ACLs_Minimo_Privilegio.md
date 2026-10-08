# Laboratorio 13 — Modelo de autorización de mínimo privilegio
**Sesión 17** · Módulo 7.2 · 25 min guiados en vivo · Informe (martes 24/11)

## Objetivo

Comprobar denegación por defecto, otorgar permisos mínimos a productor y consumidor, provocar deliberadamente el permiso de grupo olvidado, aplicar una cuota y localizar las denegaciones en el broker.

## Prerrequisitos

Laboratorio 12 completado y etapa E en ejecución en AWS/code-server. La etapa E ya declara `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false` y `User:admin-kafka` como único superusuario. No mezcle el Compose base.

Trabaje desde la raíz del repositorio:

```bash
cd ~/kafka-dian
```

Las ACLs y cuotas afectan el clúster compartido; el docente debe coordinar el orden de ejecución.

## Paso 1 — Denegación por defecto ★

```bash
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 \
  --command-config /etc/kafka/secrets/cliente.properties --list
```

**Salida esperada:** `svc-facturacion-prod` todavía no tiene permiso y recibe una autorización denegada. Solo `admin-kafka` puede administrar inicialmente.

## Paso 2 — Permisos mínimos del productor ★

Desde la raíz del repositorio:

```bash
docker exec -i broker1 bash < codigo/acls-productor.sh
docker exec broker1 /opt/kafka/bin/kafka-acls.sh --bootstrap-server broker1:19092 \
  --command-config /etc/kafka/secrets/admin.properties --list --topic dian.facturacion.emitida
```

El productor ya puede escribir en `dian.facturacion.*`, pero no leer:

```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --consumer.config /etc/kafka/secrets/cliente.properties --topic dian.facturacion.emitida \
  --from-beginning --timeout-ms 8000
```

**Salida esperada:** `TopicAuthorizationException` al intentar leer.

## Paso 3 — El grupo olvidado ★

Otorgue solo el permiso de lectura del topic; el script no concede grupos:

```bash
docker exec -i broker1 bash < codigo/acls-consumidor-topic.sh
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --consumer.config /etc/kafka/secrets/analitica.properties --group analitica-lab13 \
  --topic dian.facturacion.emitida --from-beginning --timeout-ms 8000
```

**Salida esperada:** `GroupAuthorizationException: Not authorized to access group: analitica-lab13`.

## Paso 4 — Completar el permiso de grupo ★

```bash
docker exec -i broker1 bash < codigo/acls-consumidor-grupo.sh
```

Repita el consumo: ahora funciona. El prefijo `analitica-` solo es seguro si la convención de nombres pertenece a esa dependencia y está gobernada.

## Paso 5 — Cuota por principal ★

```bash
docker exec -i broker1 bash < codigo/cuota-analitica.sh
```

Consuma un volumen suficiente y compare la duración con y sin cuota. `5 MiB/s` es un valor demostrativo, no una recomendación de producción.

## Paso 6 — Auditoría de denegaciones ★

```bash
cd ~/kafka-dian/entorno
docker compose -f docker-compose.etapaE.yml logs broker1 \
  | grep -i "denied\|Principal = User" | tail -10
```

Si el logger no expone suficiente detalle, anótelo como limitación y proponga la configuración de log/auditoría que enviaría al SIEM; no invente una línea de log que no apareció.

## Informe

Incluya las salidas de los pasos ★ y responda: (a) ¿por qué un consumidor necesita ACL de topic y grupo?; (b) ¿qué cambiaría con `allow.everyone.if.no.acl.found=true`?; (c) enumere los permisos mínimos de una aplicación Kafka Streams, incluidos sus topics internos.
