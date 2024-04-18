# The default deploy instructions (https://biffweb.com/docs/reference/production/) don't
# use Docker, but this file is provided in case you'd like to deploy with containers.
#
# When running the container, make sure you set any environment variables defined in config.env,
# e.g. using whatever tools your deployment platform provides for setting environment variables.
#
# Run these commands to test this file locally:
#
#   docker build -t your-app .
#   docker run --rm -e BIFF_PROFILE=dev -v $PWD/config.env:/app/config.env your-app
#
from clojure:temurin-17-tools-deps-bullseye

ENV TAILWIND_VERSION=v3.2.4

RUN apt-get update && apt-get install -y \
  curl default-jre \
  && rm -rf /var/lib/apt/lists/*
RUN curl -L -o /usr/local/bin/tailwindcss \
  https://github.com/tailwindlabs/tailwindcss/releases/download/$TAILWIND_VERSION/tailwindcss-linux-x64 \
  && chmod +x /usr/local/bin/tailwindcss

WORKDIR /app
COPY src ./src
COPY dev ./dev
COPY resources ./resources
COPY deps.edn .

RUN clj -M:dev uberjar && cp target/jar/app.jar . && rm -r target
RUN rm -r /usr/local/bin/tailwindcss src dev resources deps.edn

EXPOSE 8080

ENV BIFF_PROFILE=prod
ENV HOST=0.0.0.0
ENV PORT=8080
CMD ["/usr/bin/java", "-XX:-OmitStackTraceInFastThrow", "-XX:+CrashOnOutOfMemoryError", "-jar", "app.jar"]
