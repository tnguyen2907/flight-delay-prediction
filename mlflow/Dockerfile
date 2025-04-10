FROM bitnami/mlflow:2.20.1

USER root

ENV HOME=/home/mlflow
ENV PYENV_ROOT=$HOME/.pyenv
ENV PATH=$PYENV_ROOT/bin:$PATH
RUN mkdir -p $HOME && chown -R 1001:0 $HOME

RUN apt-get update && apt install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

RUN apt-get install -y default-jdk

USER mlflow

RUN git clone https://github.com/pyenv/pyenv.git ~/.pyenv

