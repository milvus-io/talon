#!/usr/bin/env python3
"""Validate the rendered multi-AZ Helm contract, including least-privilege RBAC."""
import sys
import yaml

objects = [obj for obj in yaml.safe_load_all(sys.stdin) if obj]
workloads = [obj for obj in objects if obj['kind'] in ('StatefulSet', 'Deployment')]
assert len(workloads) == 2
for workload in workloads:
    component = workload['metadata']['labels']['app.kubernetes.io/component']
    pod = workload['spec']['template']
    spec = pod['spec']
    zone = next(c for c in spec['topologySpreadConstraints']
                if c['topologyKey'] == 'topology.kubernetes.io/zone')
    assert zone['whenUnsatisfiable'] == 'DoNotSchedule' and zone['maxSkew'] == 1
    match = spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchExpressions'][0]
    assert match['key'] == 'topology.kubernetes.io/zone' and match['operator'] == 'In'
    assert zone['minDomains'] == len(match['values']) >= 2
    assert workload['spec']['replicas'] >= zone['minDomains']
    assert all(pod['metadata']['labels'][k] == v for k, v in zone['labelSelector']['matchLabels'].items())
    if component == 'worker':
        env = {item['name']: item for item in spec['containers'][0]['env']}
        assert env['TALON_NODE_NAME']['valueFrom']['fieldRef']['fieldPath'] == 'spec.nodeName'
        worker_account = spec['serviceAccountName']
role = next(obj for obj in objects if obj['kind'] == 'ClusterRole')
assert role['rules'] == [{'apiGroups': [''], 'resources': ['nodes'], 'verbs': ['get']}]
binding = next(obj for obj in objects if obj['kind'] == 'ClusterRoleBinding')
assert binding['roleRef']['name'] == role['metadata']['name']
assert any(s['kind'] == 'ServiceAccount' and s['name'] == worker_account for s in binding['subjects'])
print('multi-AZ scheduling, discovery and RBAC invariants passed')
