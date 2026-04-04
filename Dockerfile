FROM public.ecr.aws/lambda/nodejs:20

# Install sharp deps
RUN dnf install -y gcc-c++ make python3 && dnf clean all

WORKDIR /var/task

COPY package*.json ./
RUN npm install

COPY . .

# ✅ Explicitly set entrypoint + handler
ENTRYPOINT ["/lambda-entrypoint.sh"]
CMD ["index.handler"]