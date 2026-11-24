#!/usr/bin/env bash
# Run on the VM from the repository root. All checks are local to the host/network path.
set -uo pipefail

site=${1:?Pass the existing public DNS hostname}
if [[ ! $site =~ ^[A-Za-z0-9.-]+$ ]]; then
    echo "Pass a DNS hostname without a scheme or path." >&2
    exit 2
fi
compose=(docker compose -p app --env-file deploy/.env -f compose.yaml -f deploy/compose.azure.yaml)
curl_local=(curl --fail --silent --show-error --max-time 8 --resolve "$site:443:127.0.0.1")

for path in / /api/health /api/health/ready; do
    "${curl_local[@]}" --output /dev/null --write-out "path=$path status=%{http_code} latency_seconds=%{time_total}\n" "https://$site$path" ||
        echo "action=check_caddy_api_and_database path=$path"
done
"${curl_local[@]}" "https://$site/api/health/monitor" || echo 'action=inspect_api_logs'
echo

awk '/^(MemAvailable|MemTotal):/ {print}' /proc/meminfo
df -h /
cat /proc/loadavg
for service in web api scheduler; do
    id=$("${compose[@]}" ps -a -q "$service")
    if [[ -n $id ]]; then
        docker inspect --format '{{.Name}} status={{.State.Status}} restarts={{.RestartCount}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$id"
    else
        echo "action=inspect_compose service=$service state=missing"
    fi
done
docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} memory={{.MemUsage}} traffic={{.NetIO}}'
interface=$(ip route show default | awk 'NR==1 {print $5}')
if [[ -n $interface ]]; then
    for counter in rx_bytes tx_bytes; do
        printf '%s_%s=%s\n' "$interface" "$counter" "$(cat "/sys/class/net/$interface/statistics/$counter")"
    done
fi
