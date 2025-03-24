FROM openjdk:21
ADD target/upstox-algo-ws.jar upstox-algo-ws.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "upstox-algo-ws.jar", "--FILE_LOCATION=aws_env.properties"]