# First-time build can take upto 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq \
    && apt-get install firefox-esr -y -qq \
    && apt-get install wget -y -qq

COPY requirements.txt .

RUN pip install -q -r requirements.txt

# workaround to fix selenium module not found
RUN curl -sSLf "https://files.pythonhosted.org/packages/58/76/705b5c776f783d1ba7c630347463d4ae323282bbd859a8e9420c7ff79581/selenium-4.1.0-py3-none-any.whl" > ~/selenium-4.1.0-py3-none-any.whl \
    && chmod +x ~/selenium-4.1.0-py3-none-any.whl \
    && sudo pip install -q ~/selenium-4.1.0-py3-none-any.whl webdriver_manager

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID