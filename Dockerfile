FROM amazoncorretto:25
WORKDIR /app
COPY target/lastra-convert-*.jar /app/lastra-convert.jar
ENTRYPOINT ["java", "-cp", "/app/lastra-convert.jar"]
