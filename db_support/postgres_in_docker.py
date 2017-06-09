import logging
import os
import random
import subprocess
import time

from docker import Client
from docker.errors import NotFound, NullResource

from db_support.postgres import Postgres

log = logging.getLogger('[test tools pgdocker]')


class Pgdocker(Postgres):
    DB_TYPE = "pgdocker"

    def __init__(self, db_config):
        super(Pgdocker, self).__init__(db_config)

        self.image = db_config.pgdocker_image
        self.docker = Client("unix:///var/run/docker.sock")
        self.backup_path = os.path.join(db_config.backup_dir, 'default.tar')
        self._container_name = db_config.container

        # пытаемся прочесть конфиг оставшийся с последнего запуска
        self._container_file = os.path.join(db_config.backup_dir, 'pgdocker_db')
        try:
            with open(self._container_file, 'rt') as f:
                container_name, port = f.read().split(' ')
        except(FileNotFoundError, ValueError):
            # это новый контейнер
            log.info('No db')
            container_name = ''
            port = random.randint(40000, 50000)

        self._container_name = db_config.container or container_name
        self.port = str(db_config.port or port)

        try:
            self._start(container_name)
        except (NotFound, NullResource):
            # NotFound - этого контейнера больше нет на этом хосте
            # NullResource - база была удалена
            log.warning('Database container not found, you can try create new')

    def _save_container(self, new_id):
        # Если это было рандомное имя, то запоминаем его, отсекаю слеш в начале
        if new_id:
            self._container_name = self.docker.inspect_container(new_id)["Name"][1:]
        else:
            self._container_name = ''
        with open(self._container_file, 'wt') as f:
            f.write(' '.join((self._container_name, self.port)))

    def _create_container(self):
        log.debug('create container. name=%s, port=%s', self._container_name, self.port)
        return self.docker.create_container(image=self.image,
                                            name=self._container_name,
                                            detach=True,
                                            ports=[5432],
                                            host_config=self.docker.create_host_config(
                                                    port_bindings={5432: self.port}),
                                            environment={'POSTGRES_PASSWORD': self.password},
                                            )['Id']

    def _start(self, container):
        log.debug('try start db container')
        self.docker.start(container)
        # ждем около 30 секунд пока сервер начнет слушать на порту и инициализаует файловую систему
        # Сразу после поднятия psql: FATAL:  the database system is starting up
        for i in range(0, 15):
            try:
                super(Pgdocker, self)._run_console_command(['psql', '--list'], 1)
                return

            except (ConnectionRefusedError, RuntimeError, TimeoutError):
                time.sleep(2)

        raise TimeoutError('Pgdocker was not started')

    def _remove(self, container):
        log.debug('remove %s', container)
        # Контейнер не остановлен, используем флаг force
        self.docker.remove_container(container, v=True, force=True)

    def create(self):
        log.info('Create container with postgres_db. Name %s, port %s', self._container_name, self.port)
        new_container_id = self._create_container()
        try:
            self._start(new_container_id)
            super(Pgdocker, self).create()
            # параметры id контейнера меняются только если был успешный старт
            self._save_container(new_container_id)
        except Exception as e:
            self._remove(new_container_id)
            raise e

    def drop(self):
        log.info('Drop pgdocker container')
        self._remove(self._container_name)
        self._save_container(None)

    def backup(self):
        log.info('Backup database container %s on server %s', self._container_name, self.addr)
        # https://www.postgresql.org/docs/9.4/static/backup-file.html
        # The database server must be shut down in order to get a usable backup
        self.docker.stop(self._container_name, timeout=60)
        self.docker.wait(self._container_name)
        with open(self.backup_path, "wb") as f:
            (stream, stat) = self.docker.get_archive(self._container_name, "/var/lib/postgresql/data/.")
            f.write(stream.read())
        self._start(self._container_name)

    def restore(self):
        # Если это tar архив, то пробуем развернуть его как filesystem backup в остальных случаях пытаемся
        # обработать его как стандартный архив постгреса
        command = ['file', self.backup_path]
        if subprocess.check_output(command, timeout=self.quick_operation_timeout).decode(). \
                find('POSIX tar archive') != -1:
            log.info('Restore filesystem backup for container %s on server %s', self._container_name, self.addr)
            # Сначала сдедует почистить текущие файлы базы данных, для этого удаляем контейнер вместе с томом бд
            # Кроме того, при копировании бэкапа права установятся в root но видимо перепишутся при первом запуске контейнера
            self.drop()
            new_container_id = self._create_container()
            with open(self.backup_path, "rb") as f:
                self.docker.put_archive(new_container_id, "/var/lib/postgresql/data", f)
            self._start(new_container_id)
            self._save_container(new_container_id)
        else:
            super(Pgdocker, self).restore()
