FROM denoland/deno:2.1.4
WORKDIR /app
COPY server.ts .
CMD ["deno", "run", "--allow-net", "--allow-env", "server.ts"]
