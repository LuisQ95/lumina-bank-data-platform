#!/bin/bash
# Evidencia de conexión activa a Memorystore desde la VM.
# Uso: bash redis_evidencia.sh <IP_REDIS>
H="$1"
R="redis-cli -h $H"
echo "== PING =="; $R PING
echo "== INFO server (versión) =="; $R INFO server | grep -E "redis_version|uptime_in_seconds"
echo "== INFO clients =="; $R INFO clients | grep -E "connected_clients"
echo "== INFO keyspace =="; $R INFO keyspace
echo "== DBSIZE =="; $R DBSIZE
echo "== conteo de claves por prefijo =="
for p in dedup vel limites gps atm_health; do printf "%-11s %s\n" "$p:" "$($R --scan --pattern "$p:*" | wc -l)"; done
K=$($R --scan --pattern 'limites:*' | head -1)
echo "== HGETALL $K =="; $R HGETALL "$K"
echo "== TTL $K (segundos) =="; $R TTL "$K"
V=$($R --scan --pattern 'vel:*' | head -1)
echo "== ZRANGE $V (ventana de velocidad, con marca de tiempo) =="; $R ZRANGE "$V" 0 -1 WITHSCORES
G=$($R --scan --pattern 'gps:*' | head -1)
echo "== GET $G =="; $R GET "$G"
A=$($R --scan --pattern 'atm_health:*' | head -1)
echo "== HGETALL $A =="; $R HGETALL "$A"
echo "== SCARD atm_alertas =="; $R SCARD atm_alertas
echo "== HGETALL clics:resumen =="; $R HGETALL clics:resumen
