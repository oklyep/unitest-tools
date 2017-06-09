Контейнер для проектов на базе UNI

Только для тестирования

Как запустить:
1. Из директории этого файла (не забудь точку в конце):
docker build -t test-tools .
docker build -t postgres config_files/docker/postgres

2. Из любой пустой (будут созданы файлы) директории:
docker run --rm --volume $(pwd):/usr/local/test_tools_data --volume /var/run/docker.sock:/var/run/docker.sock \
-p 8080:8080 -p 8082:8082 \
--env db_rm=true \
--env jenkins_url=jenkins.somewhere \
--env jenkins_user=someuser \
--env jenkins_password=somepass \
test-tools

--rm, --env db_rm=true удалит контейнер и базу данных после завершения работы

3.
 http://localhost:8082/admin
 http://localhost:8082
