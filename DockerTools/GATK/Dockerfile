FROM java

MAINTAINER John Vivian, jtvivian@gmail.com

RUN apt-get update
RUN apt-get install -y maven git

# Obtain GATK Source
WORKDIR /home
RUN git clone https://github.com/broadgsa/gatk-protected

# Build GATK
WORKDIR /home/gatk-protected
RUN mvn package

# Move jar to /home dir.
RUN mv target/GenomeAnalysisTK.jar /home

# Set working directory and cleanup
WORKDIR /home
RUN rm -rf gatk-protected