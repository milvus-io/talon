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
