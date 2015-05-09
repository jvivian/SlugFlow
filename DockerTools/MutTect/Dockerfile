FROM java

MAINTAINER John Vivian, jtvivian@gmail.com

RUN apt-get update
RUN apt-get install -y maven git

# Create a new source directory
WORKDIR /home
RUN mkdir mutect-src
WORKDIR mutect-src

# Get Mutect source
RUN git clone git@github.com:broadinstitute/mutect.git

# Get GATK source
RUN git clone git@github.com:broadgsa/gatk-protected.git
WORKDIR gatk-protected
RUN git reset --hard 3.1

# Build GATK
mvn -Ddisable.queue install

# Build MuTect
WORKDIR /home/mutect-src/mutect
RUN mvn verify

# Move mutect.jar to /home directory
RUN mv target/mutect* /home

# Set working directory and cleanup
WORKDIR /home
RUN rm -rf mutect-src
