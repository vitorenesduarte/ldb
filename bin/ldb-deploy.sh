#!/bin/bash

DCOS=$(dcos config show core.dcos_url)
TOKEN=$(dcos config show core.dcos_acs_token)

ENV_VARS=(
  BRANCH
  LDB_MODE
  LDB_JOIN_DECOMPOSITIONS
  LDB_DCOS_OVERLAY
  LDB_SIMULATION
  LDB_NODE_NUMBER
  LDB_EVALUATION_IDENTIFIER
  LDB_EVALUATION_TIMESTAMP
  LDB_INSTRUMENTATION
  LDB_EXTENDED_LOGGING
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

MEMORY=512.0
CPU=0.6

cat <<EOF > ldbs.json
{
  "acceptedResourceRoles": [
    "slave_public"
  ],
  "id": "ldbs",
  "dependencies": [],
  "cpus": $CPU,
  "mem": $MEMORY,
  "instances": $LDB_NODE_NUMBER,
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
    "TOKEN": "$TOKEN",
    "LDB_MODE": "$LDB_MODE",
    "LDB_JOIN_DECOMPOSITIONS": "$LDB_JOIN_DECOMPOSITIONS",
    "LDB_DCOS_OVERLAY": "$LDB_DCOS_OVERLAY",
    "LDB_SIMULATION": "$LDB_SIMULATION",
    "LDB_NODE_NUMBER": "$LDB_NODE_NUMBER",
    "LDB_EVALUATION_IDENTIFIER": "$LDB_EVALUATION_IDENTIFIER",
    "LDB_EVALUATION_TIMESTAMP": "$LDB_EVALUATION_TIMESTAMP",
    "LDB_INSTRUMENTATION": "$LDB_INSTRUMENTATION",
    "LDB_EXTENDED_LOGGING": "$LDB_EXTENDED_LOGGING"
  },
  "healthChecks": []
}
EOF

echo ">>> Adding LDBs to Marathon"
curl -s -H "Authorization: token=$TOKEN" -H 'Content-type: application/json' -X POST -d @ldbs.json "$DCOS/service/marathon/v2/apps" > /dev/null
