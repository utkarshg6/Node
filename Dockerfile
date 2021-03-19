# *** Build Stage ***
FROM rust:latest AS builder

# Update OS
RUN apt-get update && \
    apt-get -y upgrade

# Download MASQ Node source
# RUN git clone https://github.com/MASQ-Project/Node.git src
COPY ./ src

# Compile MASQNode
RUN cd /src/node/ && \
    cargo build --release --verbose
RUN mkdir /build && \
    cp /src/node/target/release/MASQNode /build/

# Compile masq
RUN cd /src/masq/ && \
    cargo build --release --verbose && \
    cp /src/node/target/release/masq /build/

# Compile dns_utility
RUN cd /src/dns_utility/ && \
    cargo build --release --verbose && \
    cp /src/dns_utility/target/release/dns_utility /build/

# Compile port_exposer
RUN cd /src/port_exposer/ && \
    cargo build --release --verbose && \
    cp /src/port_exposer/target/release/port_exposer /build/

# *** Serve Stage ***
FROM debian:buster-slim as server

LABEL maintainer="microoo <hu@microoo.net>"

# Update OS
RUN apt-get update && \
    apt-get -y upgrade && \ 
    apt-get -y install python3 python3-pip && \
    pip3 install requests tomlkit

# Create a sudo user
RUN apt-get install sudo -y && \
    adduser --disabled-password --gecos '' node && \
    adduser node sudo && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Install MASQ Node
COPY --from=builder --chown=node:node /build/* /usr/local/bin/
COPY MASQ.toml /etc/MASQ.toml
COPY ci/helper.py /usr/local/bin/masq_helper.py

## Run MASQ Node
CMD [ "bash", "-c", "python3 /usr/local/bin/masq_helper.py && sudo MASQNode --config-file /etc/MASQ.toml" ]
USER node
WORKDIR /home/node

# Install MASQ Node
COPY --from=builder --chown=node:node /build/* /usr/local/bin/