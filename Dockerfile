FROM python:3.13-alpine AS build
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir build && python -m build --wheel --outdir /dist

FROM python:3.13-alpine
LABEL org.opencontainers.image.source="https://github.com/natindonesia/granian-k8s-autoscaler" \
      org.opencontainers.image.description="Shock-resistant autoscaling signals and reflex for Granian web pods and RQ workers on Kubernetes" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl \
    && adduser -D -u 65532 scaler
USER 65532
EXPOSE 8080
ENTRYPOINT ["granian-k8s-autoscaler"]
CMD ["run", "--config", "/etc/granian-k8s-autoscaler/config.json"]
