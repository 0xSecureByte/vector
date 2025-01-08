FROM debian:latest

# Set the working directory
WORKDIR /app

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcc \
    libc6-dev \
    libsasl2-dev \
    libssl-dev \
    pkg-config \
    git \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set the default command to run when the container starts
CMD ["bash"]