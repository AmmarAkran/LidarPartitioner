
FROM continuumio/miniconda3:4.5.4

RUN echo "python==3.6.10" >> /opt/conda/conda-meta/pinned

ENV FLASK_PROXY_PORT 8080
ENV GDAL_DATA /opt/conda/share/gdal
ENV PROJ_LIB /opt/conda/share/proj

RUN apt-get update\
        # Upgrade installed packages to get latest security fixes if the base image does not contain them already.
        && apt-get upgrade -y --no-install-recommends \
        # add some packages required for the pip install
        && apt-get install -y --no-install-recommends \
	   gdal-bin \
           gcc \
           libc-dev \
           libxslt-dev \
           libxml2-dev \
           libffi-dev \
           libssl-dev \
           zip \
           unzip \
           vim \
           make \
	   build-essential cmake\
        # cleanup package lists, they are not used anymore in this image
        && rm -rf /var/lib/apt/lists/* \
        && apt-cache search linux-headers-generic


RUN apt-cache search linux-headers-generic

# -n base

RUN conda update conda && conda install -c conda-forge pdal python-pdal gdal && conda clean --all


ENV PATH /opt/conda/bin:$PATH
ENV PATH /opt/conda/share:$PATH
ENV PATH /opt/conda/share/proj/bin:$PATH

#COPY requirements.txt requirements.txt
COPY . .

RUN pip install --upgrade pip setuptools six && pip install --no-cache-dir -r requirements.txt
RUN pip install --pre laspy[lazrs,laszip]

WORKDIR ./neilpy-master
RUN pip install .


# INSTALL compilers and build toold

RUN apt-get update && apt-get install -y git cmake build-essential gcc g++

# create lastools working directory

WORKDIR /opt

RUN wget http://www.cs.unc.edu/~isenburg/lastools/download/lastools.zip
RUN apt-get install -y unzip
RUN unzip lastools.zip

WORKDIR /opt/LAStools/

RUN make
# RUN ln -s /opt/LAStools/bin/lasinfo /usr/local/sbin/lasinfo
# RUN ln -s /opt/LAStools/bin/lasmerge /usr/local/sbin/lasmerge

ENV PATH /opt/LAStools/bin/:$PATH


WORKDIR /

# create action working directory
RUN mkdir -p /action \
    && mkdir -p /actionProxy \
    && mkdir -p /pythonAction

ADD https://raw.githubusercontent.com/apache/openwhisk-runtime-docker/8b2e205c39d84ed5ede6b1b08cccf314a2b13105/core/actionProxy/actionproxy.py /actionProxy/actionproxy.py
ADD https://raw.githubusercontent.com/apache/openwhisk-runtime-python/3%401.0.3/core/pythonAction/pythonrunner.py /pythonAction/pythonrunner.py

CMD ["/bin/bash", "-c", "cd /pythonAction && python -u pythonrunner.py"]

