FROM tomcat:8-jre8

RUN apt-get update && apt-get install -y \
 python3 \
 python3-dev \
 python3-setuptools \
 python3-pip \
 postgresql-client \
 freetds-dev

# support unicode
RUN apt-get install -y locales && \
 locale-gen C.UTF-8 && \
 /usr/sbin/update-locale LANG=C.UTF-8

RUN pip3 install --upgrade pip \
 docker-py==1.10 \
 jenkinsapi==0.3 \
 pymssql==2.1 \
 pytz \
 python-magic \
 tornado==4.5

# add WORK_DIR
VOLUME /usr/local/test_tools_data

# add tomcat symlink for uni
RUN rm -rf /usr/local/tomcat/webapps/ROOT
RUN ln -s /usr/local/test_tools_data/webapp /usr/local/tomcat/webapps/ROOT

# add this
COPY . /usr/local/test_tools

ENV TZ=Asia/Yekaterinburg

# uni, java debug, test-tools
EXPOSE 8080
EXPOSE 8081
EXPOSE 8082

CMD ["python3", "/usr/local/test_tools/main.py"]
