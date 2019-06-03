# Not final version of dockerfile for assignment

FROM python:3.7.3-alpine3.9

RUN pip install flask
RUN pip install requests

COPY . .

EXPOSE 8080
CMD ["python", "rest.py", "-p 8080"]
