FROM      python:3.8-slim

LABEL     Name="oreilly_app" \
          Author="Brad Frank" \
          Maintainer="bradfrank@fastmail.com" \
          Description="Rest API for O'Reilly books."

RUN       mkdir /app
COPY      app/ /app/
RUN       chmod 0755 /app/bootstrap.py

RUN       pip install -r /app/requirements.txt

CMD       [ "/app/bootstrap.py" ]