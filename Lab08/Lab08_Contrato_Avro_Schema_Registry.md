# Laboratorio 08 — Contrato Avro del evento de declaración
**Sesión 10** · Módulo 5.1 · 40 min guiados en vivo · Informe (martes 27/10)

## Objetivo
Levantar la etapa C del entorno, definir y registrar el esquema Avro del evento de declaración, migrar productor y consumidor a Avro, inspeccionar el mensaje en crudo e intentar producir un evento inválido.

## Prerrequisitos
```bash
# En AWS/code-server, desde la carpeta del entorno
cd ~/kafka-dian/entorno
docker compose -f docker-compose-lab-kafka.yml -f docker-compose.etapaC.yml up -d
curl -s localhost:8081/subjects   # debe responder []
cd ~/kafka-dian
python3 -m pip install "confluent-kafka[avro,schemaregistry]"
```

En la terminal de code-server, `localhost` apunta a la instancia EC2 y sirve para `curl`. Desde el navegador use las URLs públicas publicadas por el docente.

## Paso 1 — Revisar el esquema ★
Abra `declaracion-presentada-v1.avsc`. Verifique: `namespace` por dominio, `doc` en **cada** campo, tipos lógicos (`timestamp-millis`, `decimal`) y el `enum` de estado. Responda en el informe: ¿qué impide el `enum` que un `string` permitiría?

## Paso 2 — Registrar el esquema ★
```bash
JSON=$(python3 -c "import json;print(json.dumps({'schema':open('codigo/declaracion-presentada-v1.avsc').read()}))")
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$JSON" http://localhost:8081/subjects/dian.declaracion.presentada-value/versions
curl -s http://localhost:8081/subjects/dian.declaracion.presentada-value/versions
curl -s http://localhost:8081/config/dian.declaracion.presentada-value
```
**Salida esperada:** `{"id":1}`, luego `[1]`, y la política `BACKWARD` (heredada del valor global del compose).

## Paso 3 — Producir y consumir con Avro ★
```bash
EQUIPO=equipo01
python3 codigo/productor_avro.py --n 50
python3 codigo/consumidor_avro.py --group "${EQUIPO}-lab08"
```
**Salida esperada:** `enviados: 50` y luego cinco líneas con `nit`, `formulario`, `valor` y `estado` ya tipados. Note que el consumidor **no** recibe el esquema como parámetro: lo recupera del registro con el ID de la cabecera.

## Paso 4 — El mensaje en crudo ★
```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --topic dian.declaracion.presentada --from-beginning --max-messages 1 | xxd | head -4
```
**Observe:** el primer byte es `00` (byte mágico) y los cuatro siguientes son el ID del esquema en big-endian (`00 00 00 01` = esquema 1). El resto es Avro binario, ilegible sin el esquema.
**Para el informe:** explique por qué el esquema no viaja completo en cada mensaje y cuánto ocupa la cabecera.

## Paso 5 — Un evento que viola el esquema ★
```bash
python3 codigo/productor_avro.py --n 5 --invalido
```
**Salida esperada:** `[RECHAZADO POR EL ESQUEMA] … EN_TRAMITE not in enum EstadoDeclaracion` y `enviados: 0`.
**Punto clave:** la validación ocurre **en el cliente, antes de la red**. El broker nunca vio el mensaje inválido.

## Paso 6 — Consultar el esquema por ID
```bash
curl -s http://localhost:8081/schemas/ids/1 | python3 -m json.tool | head -20
```
Es exactamente lo que hace el deserializador.

## Informe
Salidas de los pasos ★ y respuestas a: (a) ¿qué impide el `enum`?; (b) ¿por qué solo viaja el ID?; (c) ¿qué habría pasado en la sesión 7, con JSON sin esquema, al producir `EN_TRAMITE`?; (d) proponga dos campos que agregaría al esquema para su propio dominio y redacte su `doc`.
