#!/bin/bash

DCOS=$(dcos config show core.dcos_url)
TOKEN=$(dcos config show core.dcos_acs_token)

ENV_VARS=(
  BRANCH
)

for ENV_VAR in "${ENV_VARS[@]}"
do
  if [ -z "${!ENV_VAR}" ]; then
    echo ">>> ${ENV_VAR} is not configured. Please export it."
    exit 1
  fi
done

echo ">>> Configuring LDBs"
cd /tmp

MEMORY=2048.0
CPU=2
INSTANCES=5

cat <<EOF > ldbs.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "ldbs",
  "dependencies": [],
  "constraints": [["hostname", "UNIQUE", ""]],
  "cpus": $CPU,
  "mem": $MEMORY,
  "instances": $INSTANCES,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "vitorenesduarte/ldb",
      "network": "HOST",
      "forcePullImage": true,
      "parameters" : [
        { "key": "oom-kill-disable", "value": "true" }
      ]
    }
  },
  "ports": [0],
  "env" : {
    "BRANCH": "$BRANCH",
    "DCOS": "$DCOS",
    "TOKEN": "$TOKEN"
  },
  "healthChecks": [
    {
      "path": "/api/health",
      "portIndex": 0,
      "protocol": "HTTP",
      "gracePeriodSeconds": 300,
      "intervalSeconds": 60,
      "timeoutSeconds": 20,
      "maxConsecutiveFailures": 3,
      "ignoreHttp1xx": false
    }
  ]
}
EOF

echo ">>> Adding LDBs to Marathon"
curl -s -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @ldbs.json "$DCOS/service/marathon/v2/apps" > /dev/null
