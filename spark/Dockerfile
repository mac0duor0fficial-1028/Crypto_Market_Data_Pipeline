FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
# Shared, writable dirs: checkpoints (also mounted in the worker so executor state-store
# files land on the same volume as the driver's offsets) and the Ivy cache for --packages.
RUN mkdir -p /checkpoints /tmp/.ivy /opt/jobs /opt/schemas \
 && chown -R spark:spark /checkpoints /tmp/.ivy /opt/jobs /opt/schemas
USER spark
