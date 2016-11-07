#!/bin/bash
IP=$(curl -s -H "Authorization: token="$(dcos config show core.dcos_acs_token) $(dcos config show core.dcos_url)/service/marathon/v2/apps/ldb-mongo/tasks | grep -oE "\"ipAddress\":\"[0-9\.]+\"" | grep -oE "[0-9\.]+")
PORT=$(curl -s -H "Authorization: token="$(dcos config show core.dcos_acs_token) $(dcos config show core.dcos_url)/service/marathon/v2/apps/ldb-mongo/tasks | grep -oE "\"ports\":\[[0-9]+\]" | grep -oE "[0-9]+")

echo "IP: $IP"
echo "PORT: $PORT"
