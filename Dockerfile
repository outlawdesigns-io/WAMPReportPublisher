FROM node:latest
WORKDIR /usr/src/app/
ARG ENV
ENV TZ=America/Chicago
ENV NODE_ENV=$ENV
RUN mkdir -p /mnt/LOE/log
RUN echo America/Chicago > /etc/timezone
RUN ln -sf /usr/share/zoneinfo/America/Chicago /etc/localtime
RUN dpkg-reconfigure -f noninteractive tzdata
COPY . .
RUN npm install
CMD ["/bin/sh","-c","npm start > /mnt/LOE/log/loereports.log"]
