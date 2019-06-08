# Not final version of dockerfile for assignment

FROM python:3.7.3-alpine3.9

# Make hash() call deterministic for sharding
ENV PYTHONHASHSEED 1

RUN pip install flask
RUN pip install requests

COPY . .

EXPOSE 8080
CMD ["python", "rest.py"]