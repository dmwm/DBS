FROM cern/cc7-base
MAINTAINER Yuyi Guo yuyi.guo.usa@gmail.com

ENV WDIR=/data
ENV USER=_dbs
ENV VER=HG1805a

ADD cernonly2.repo /etc/yum.repos.d/cernonly2.repo

RUN yum update -y && yum clean all
RUN yum install -y git-core zip unzip which file bzip2 e2fsprogs e2fsprogs-libs compat-libstdc++-33 \
    openssl iopenssl-libs openssh openssh-clients \
    perl-Thread-Queue zsh tk freetype perl-ExtUtils-Embed fontconfig \
    perl-Test-Harness perl-Data-Dumper perl-Digest-MD5 perl-Switch perl-Env \
    libX11-devel libX11 libXmu libSM libICE libXcursor libXext libXrandr libXft \
    mesa-libGLU mesa-libGL libXi libXinerama libXft-devel libXrender libXpm \
    libXpm-devel libXext-devel mesa-libGLU-devel libaio
RUN yum clean all

# add new user
RUN useradd ${USER} && install -o ${USER} -d ${WDIR}
USER ${USER}
ADD install.sh $WDIR/install.sh
ADD start.sh $WDIR/start.sh

# start the setup
WORKDIR ${WDIR}

# install
RUN $WDIR/install.sh

#remove dumy file, the real file sits on the host.
RUN rm ${WDIR}/srv/${VER}/auth/dbs/DBSSecrets.py

#Define the current
RUN cd ${WDIR}/srv && ln -s ${VER} current

RUN cd ${WDIR}/srv/HG1805a && ln -s apps.sw apps

RUN cd ${WDIR}/srv/enabled && touch dbs
# run the service
USER $USER
WORKDIR $WDIR
CMD ["$WDIR/start.sh"]
