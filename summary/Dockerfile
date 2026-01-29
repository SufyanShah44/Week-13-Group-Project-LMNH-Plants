FROM public.ecr.aws/lambda/python:latest

WORKDIR ${LAMBDA_TASK_ROOT}

COPY ./pipeline/requirements.txt .

RUN pip install -r requirements.txt

COPY summary/summary.py .

CMD ["summary.lambda_handler"]