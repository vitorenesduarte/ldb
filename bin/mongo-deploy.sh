#!/bin/bash

DCOS=$(dcos config show core.dcos_url)
TOKEN=$(dcos config show core.dcos_acs_token)

echo ">>> Configuring Mongo"
cd /tmp

MEMORY=2048.0
CPU=2

cat <<EOF > mongo.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "ldb-mongo",
  "dependencies": [],
  "constraints": [["hostname", "UNIQUE", ""]],
  "cpus": $CPU,
  "mem": $MEMORY,
  "instances": 1,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "vitorenesduarte/mongo",
      "network": "HOST",
      "forcePullImage": true,
      "parameters" : [
        { "key": "oom-kill-disable", "value": "true" }
      ]
    }
  },
  "ports": [0],
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

echo ">>> Adding Mongo to Marathon"
curl -s -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @mongo.json "$DCOS/service/marathon/v2/apps" > /dev/null
