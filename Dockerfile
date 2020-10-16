FROM amazoncorretto:8

VOLUME /tmp
ARG APP_JAR_FILE
ARG APP_CLASS=com.totango.App
ENV MAIN_CLASS=${APP_CLASS}

RUN mkdir -p /app/lib

COPY ./target/lib /app/lib
COPY ./target/${APP_JAR_FILE} /app/lib/

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -cp \"app/lib/*\" ${MAIN_CLASS} ${APP_ARGS}"]