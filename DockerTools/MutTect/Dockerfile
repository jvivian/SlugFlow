FROM java

MAINTAINER John Vivian, jtvivian@gmail.com

RUN apt-get update
RUN apt-get install -y maven git

# Create a new source directory
WORKDIR /home
RUN mkdir mutect-src
WORKDIR mutect-src

# Get Mutect source
RUN git clone https://github.com/broadinstitute/mutect

# Get GATK source
RUN git clone https://github.com/broadgsa/gatk-protected
WORKDIR gatk-protected
RUN git reset --hard 3.1

# Build GATK
RUN mvn -Ddisable.queue install

# Build MuTect
WORKDIR /home/mutect-src/mutect
RUN mvn verify

# Move mutect.jar to /home directory
RUN mv target/mutect* /home

# Set working directory and cleanup
WORKDIR /home
RUN rm -rf mutect-src
