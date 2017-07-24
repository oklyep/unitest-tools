import logging
import time
import os
import shutil
import subprocess
from contextlib import contextmanager

from config import RootConfig
from db_support.mssql import Mssql
from db_support.postgres import Postgres
from db_support.postgres_in_docker import Pgdocker
from jenkins import Jenkins

log = logging.getLogger('[test tools main]')


class Engine(object):
    CREATE_DB = 'CREATE_DB'
    RESTORE_DB = 'RESTORE_DB'
    BACKUP_DB = 'BACKUP_DB'
    REDUCE_DB = 'REDUCE_DB'
    DROP_DB = 'DROP_DB'
    BUILD = 'BUILD'
    UPLOAD = 'UPLOAD'

    def __init__(self, config: RootConfig):
        self.config = config
        self._create_dirs()

        if config.db_type == Postgres.DB_TYPE:
            self.db = Postgres(config.db)

        elif config.db_type == Mssql.DB_TYPE:
            self.db = Mssql(config.db)

        elif config.db_type == Pgdocker.DB_TYPE:
            self.db = Pgdocker(config.db)

        else:
            raise RuntimeError('Unsupported database type')
        self._write_hibernate_properties()

        self.jenkins = Jenkins(config.jenkins)

        os.environ['CATALINA_OPTS'] = config.catalina_opts
        self.tomcat = None
        self.last_error = None
        self.active_task = None
        self.last_task = None

        log.info('Test tools started')

    def _create_dirs(self):
        log.debug('Create dirs')
        if not os.path.exists(self.config.WORK_DIR):
            os.mkdir(self.config.WORK_DIR)

        if not os.path.exists(self.config.UNI_CONFIG_DIR):
            shutil.copytree(self.config.UNI_TEMPLATE_CONFIG_DIR, self.config.UNI_CONFIG_DIR)

        if not os.path.exists(self.config.UNI_WEBAPP):
            os.mkdir(self.config.UNI_WEBAPP)

    def _write_hibernate_properties(self):
        log.debug('Create hibernate file')
        if isinstance(self.db, Mssql):
            pattern_file = self.config.UNI_TEMPLATE_MSSQL
        else:
            pattern_file = self.config.UNI_TEMPLATE_POSTGRES

        with open(pattern_file) as f:
            conf = f.read()

        conf = conf.format(addr=self.db.addr, name=self.db.name, port=self.db.port,
                           user=self.db.user, password=self.db.password)

        if not self.config.db.validate_entity_code:
            conf += '\ndb.validateEntityCode=false\n'

        with open(self.config.UNI_CONFIG_DB_FILE, 'wt') as f:
            f.write(conf)

    def _write_version_file(self, build_details):
        log.debug('Write version file')
        with open(self.config.UNI_VERSION_FILE, 'wt') as f:
            f.writelines(['Test tools. Jenkins job: {} {} at {}'.format(
                    self.jenkins.project,
                    self.jenkins.version or '',
                    build_details),
            ])

    def exit(self):
        log.info('Shutdown...')
        self.stop_tomcat()
        if self.config.db.rm:
            try:
                self.db.drop()
            except Exception as e:
                log.exception(e)

    def start_tomcat(self):
        log.info('start tomcat')
        # Если процесс существует и запущен - то не делать ничего
        if self.tomcat is not None and self.tomcat.poll() is None:
            return

        self.tomcat = subprocess.Popen([self.config.CATALINA_SH, "run"])
        # Иначе кто-то может остановить томкат сразу после запуска, что вызовет рождение зомби uname, dirname, tty
        time.sleep(2)

    def stop_tomcat(self):
        log.info('stop tomcat')
        # Если процесс не существует или уже остановлен - то не делать ничего
        if self.tomcat is None or self.tomcat.poll() is not None:
            return
        try:
            self.tomcat.terminate()
            self.tomcat.wait(30)
        except subprocess.TimeoutExpired:
            self.tomcat.kill()

    def log_exceptions(self, runnable):
        try:
            runnable()
            # Убрать предудущую индикацию об ошибке. Иначе вообще непонятно будет все хорошо или не очень
            self.last_error = None
        except Exception as e:
            self.last_error = str(e)
            log.exception(e)

    @contextmanager
    def _new_task(self, task_name):
        self.active_task = task_name
        log.info("Task %s started", task_name)
        try:
            yield
        finally:
            self.last_task = task_name
            self.active_task = None
            log.info("Task finished")

    def engine_status(self):
        if self.tomcat is not None:
            returncode = self.tomcat.poll()
        else:
            returncode = 0

        with open(self.config.UNI_VERSION_FILE, 'rt') as f:
            uni_version = f.read()

        return {
            "last_error": self.last_error,
            "last_task": self.last_task,
            "active_task": self.active_task,
            "db_addr": self.db.addr,
            "tomcat_returncode": returncode,
            'uni_version': uni_version,
        }

    def new_db(self):
        self.stop_tomcat()
        with self._new_task(self.CREATE_DB):
            self.db.create()
            self._write_hibernate_properties()
        # если есть бэкап - восстанавливаем, иначе будет создана пустая база при первом запуске
        if self.db.has_default_backup():
            self.restore()

    def drop_db(self):
        self.stop_tomcat()
        with self._new_task(self.DROP_DB):
            self.db.drop()

    def restore(self):
        self.stop_tomcat()
        with self._new_task(Engine.RESTORE_DB):
            self.db.restore()
            self.db.set_1_1()

    def backup(self):
        self.stop_tomcat()
        with self._new_task(Engine.BACKUP_DB):
            self.db.backup()

    def reduce(self):
        self.stop_tomcat()
        with self._new_task(Engine.REDUCE_DB):
            self.db.reduce()
            self.db.customer_patch()

    def update(self, build=None):
        self.stop_tomcat()
        with self._new_task(Engine.UPLOAD):
            self._write_version_file(self.jenkins.get_build(self.config.UNI_WEBAPP, build))
        self.start_tomcat()

    def build_and_update(self):
        with self._new_task(Engine.BUILD):
            build = self.jenkins.build_project()
        self.update(build)
