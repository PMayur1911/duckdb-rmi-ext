FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# -----------------------------
#  ccache setup
# -----------------------------
ENV CCACHE_DIR=/ccache
ENV CCACHE_MAXSIZE=5G
ENV CCACHE_COMPRESS=1

# -----------------------------
#  Install build dependencies
# -----------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    ccache \
    git \
    python3 \
    python3-pip \
    ca-certificates \
    libssl-dev \
    libreadline-dev \
    zlib1g-dev \
    ripgrep \
    gdb \
    bash-completion \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
#  Ninja as default generator
# -----------------------------
ENV CMAKE_GENERATOR=Ninja
ENV PATH="/usr/lib/ccache:${PATH}"

# -----------------------------
#  Working directory
# -----------------------------
WORKDIR /workspace

# -----------------------------
#  Developer shell QoL
# -----------------------------
RUN echo 'PS1="(duckdb-ext) \\u@\\h:\\w$ "' >> /root/.bashrc && \
    echo 'alias ll="ls -alF --color=auto"' >> /root/.bashrc && \
    echo 'export PATH="/usr/lib/ccache:${PATH}"' >> /root/.bashrc

# -----------------------------
#  Done — extension development happens inside /workspace
# -----------------------------
