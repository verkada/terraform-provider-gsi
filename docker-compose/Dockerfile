FROM openjdk:16.0.1
RUN mkdir -p opt/dynamodb
WORKDIR /opt/dynamodb
RUN wget https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz -q -O - | tar -xz
EXPOSE 8000
ENTRYPOINT ["java", "-jar", "DynamoDBLocal.jar"]
