# # To enable ssh & remote debugging on app service change the base image to the one below
# # FROM mcr.microsoft.com/azure-functions/python:4-python3.11-appservice
FROM mcr.microsoft.com/azure-functions/python:4-nightly-python3.11

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libreoffice \
    libssl-dev \
    python-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /
RUN pip install -r /requirements.txt


COPY . /home/site/wwwroot

# # Dockerfile - WIP
# # Use the base image for Azure Functions with Python 3.11
# FROM mcr.microsoft.com/azure-functions/python:4-nightly-python3.11

# # Set environment variables
# ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
#     AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# # Remove old Maven packages and related libraries
# RUN apt-get update && \
#     apt-get purge -y maven maven2 maven3 libmaven-shared-utils-java || true && \
#     rm -rf /usr/share/maven* /usr/share/java/maven* /usr/bin/mvn* && \
#     rm -rf /opt/azure-functions-host/tools/maven

# # Install Java, curl, and other dependencies
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     libffi-dev \
#     libreoffice \
#     libssl-dev \
#     python-dev \
#     openjdk-17-jdk \
#     curl 

# # Install Maven 3.9.5 manually
# ARG MAVEN_VERSION=3.9.5
# ARG MAVEN_BASE_URL=https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries
# RUN curl -fsSL ${MAVEN_BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz -o /tmp/maven.tar.gz && \
#     tar -xzf /tmp/maven.tar.gz -C /opt && \
#     ln -s /opt/apache-maven-${MAVEN_VERSION} /opt/maven && \
#     rm /tmp/maven.tar.gz

# # Set environment variables for Maven
# ENV MAVEN_HOME=/opt/maven
# ENV PATH=${MAVEN_HOME}/bin:${PATH}

# # Verify Maven version
# RUN mvn --version

# # Install Python dependencies
# COPY requirements.txt /
# RUN pip install -r /requirements.txt

# RUN rm -rf /usr/share/java/maven* /usr/share/java/plexus* /usr/share/java/jsoup* /usr/share/java/commons-compress*

# # Copy application code
# COPY . /home/site/wwwroot

