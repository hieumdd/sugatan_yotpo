FROM joyzoursky/python-chromedriver:3.9

ARG PYTHON_ENV

ENV PYTHONUNBUFFERED=1
ENV PYTHONFAULTHANDLER=1
ENV PYTHONHASHSEED=random
ENV PIP_NO_CACHE_DIR=off
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_DEFAULT_TIMEOUT=100

# RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
RUN pip install poetry
WORKDIR /app

COPY poetry.lock pyproject.toml /app/

RUN poetry config virtualenvs.create false \
  && poetry install $(test "$PYTHON_ENV" == prod && echo "--no-dev") --no-root --no-interaction --no-ansi

COPY . /app

EXPOSE 8080

ENV PORT=8080

CMD exec functions-framework --target=main
