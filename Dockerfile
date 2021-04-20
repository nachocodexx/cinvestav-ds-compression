FROM openjdk:8
COPY ./target/scala-2.13/storage-pool.jar /app/src/app.jar
WORKDIR /app/src
ENTRYPOINT ["java", "-jar","app.jar"]