ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}

ARG PIP_INDEX_URL=

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /dashboard

COPY pyproject.toml README.md /dashboard/

RUN python -c 'import tomllib, pathlib; print("\n".join(tomllib.loads(pathlib.Path("pyproject.toml").read_text())["project"]["dependencies"]))' > /tmp/runtime-requirements.txt \
    && if [ -n "$PIP_INDEX_URL" ]; then \
        pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -r /tmp/runtime-requirements.txt; \
    else \
        pip install --no-cache-dir -r /tmp/runtime-requirements.txt; \
    fi

COPY app /dashboard/app

EXPOSE 8090

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090"]
