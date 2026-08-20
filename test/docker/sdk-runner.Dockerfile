# In-cluster runner for the multi-language SDK e2e: Python (pytest) + a JRE for
# the compiled Java suite, plus gcc to build the C suite inside the pod (so the
# C binary is linked against the pod's own glibc, not the host's). The base
# matches the coordinator/worker images, so the FROM layer is already pulled.
#
# No CMD/ENTRYPOINT on purpose: this is a pure environment image. The "hold the
# pod open" command lives in the pod spec (test/stack/deploy.sh), not here.
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip openjdk-17-jre-headless gcc libc6-dev \
    && pip install --no-cache-dir --break-system-packages pytest \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /e2e
