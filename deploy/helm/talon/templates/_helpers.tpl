{{/*
Common helpers for the Talon chart.
*/}}

{{- define "talon.name" -}}
talon
{{- end -}}

{{/*
Standard labels applied to every object.
*/}}
{{- define "talon.labels" -}}
app.kubernetes.io/name: talon
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: talon-{{ .Chart.Version }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Component selector labels (name + component only — stable across upgrades).
*/}}
{{- define "talon.selectorLabels" -}}
app.kubernetes.io/name: talon
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Resolve the image tag: explicit override or the chart appVersion.
*/}}
{{- define "talon.imageTag" -}}
{{- .Values.image.tag | default .Chart.AppVersion -}}
{{- end -}}

{{- define "talon.coordinatorImage" -}}
{{ .Values.image.registry }}/{{ .Values.image.coordinator }}:{{ include "talon.imageTag" . }}
{{- end -}}

{{- define "talon.workerImage" -}}
{{ .Values.image.registry }}/{{ .Values.image.worker }}:{{ include "talon.imageTag" . }}
{{- end -}}

{{/*
Validate the state backend and HA/replica invariants once, early.
*/}}
{{- define "talon.validateBackend" -}}
{{- $b := .Values.coordinator.backend -}}
{{- if not (has $b (list "memory" "kubernetes" "etcd")) -}}
{{- fail (printf "coordinator.backend must be one of memory|kubernetes|etcd, got %q" $b) -}}
{{- end -}}
{{- if and (eq $b "memory") (gt (int .Values.coordinator.replicas) 1) -}}
{{- fail "coordinator.backend=memory does not support HA; set coordinator.replicas=1 or choose kubernetes/etcd" -}}
{{- end -}}
{{- end -}}

{{/*
Effective coordinator replica count (memory backend is single-node).
*/}}
{{- define "talon.coordinatorReplicas" -}}
{{- if eq .Values.coordinator.backend "memory" -}}1{{- else -}}{{ .Values.coordinator.replicas }}{{- end -}}
{{- end -}}

{{/* Shared zone placement; selectors count only this release's component. */}}
{{- define "talon.zoneSpread" -}}
- maxSkew: 1
  minDomains: {{ len .root.Values.multiAz.zones }}
  topologyKey: topology.kubernetes.io/zone
  whenUnsatisfiable: DoNotSchedule
  labelSelector:
    matchLabels:
      app.kubernetes.io/instance: {{ .root.Release.Name }}
      {{- include "talon.selectorLabels" (dict "component" .component) | nindent 6 }}
{{- end -}}

{{- define "talon.zoneNodeAffinity" -}}
nodeAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
      - matchExpressions:
          - key: topology.kubernetes.io/zone
            operator: In
            values:
              {{- toYaml .Values.multiAz.zones | nindent 14 }}
{{- end -}}

{{- define "talon.validateMultiAz" -}}
{{- if .Values.multiAz.enabled -}}
{{- if not (semverCompare ">=1.30-0" .Capabilities.KubeVersion.Version) -}}
{{- fail "multiAz requires Kubernetes 1.30+ (minDomains)" -}}
{{- end -}}
{{- $zones := .Values.multiAz.zones -}}
{{- if or (lt (len $zones) 2) (ne (len $zones) (len (uniq $zones))) -}}
{{- fail "multiAz.zones must list at least two distinct zones" -}}
{{- end -}}
{{- range $zone := $zones -}}
{{- if not (regexMatch "^[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$" $zone) -}}
{{- fail "multiAz.zones entries must be valid nonempty Kubernetes label values" -}}
{{- end -}}
{{- end -}}
{{- if or (eq .Values.coordinator.backend "memory") (lt (int .Values.coordinator.replicas) (len $zones)) -}}
{{- fail "multiAz requires an HA backend and at least one coordinator per zone" -}}
{{- end -}}
{{- if and .Values.worker.enabled (lt (int .Values.worker.replicas) (len $zones)) -}}
{{- fail "multiAz requires at least one worker per zone" -}}
{{- end -}}
{{- end -}}
{{- end -}}
