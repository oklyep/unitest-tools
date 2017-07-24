import json
import logging
import logging.config
import os

from db_support.mssql import Mssql
from db_support.postgres import Postgres
from db_support.postgres_in_docker import Pgdocker

log = logging.getLogger('[test tools config]')


class ConfigObject(object):
    UNDEFINED = 'undefined'
    CONF_NAME = 'base config'

    def _update_from_dict(self, d):
        for key, val in d.items():
            if key in self.__dict__:
                if isinstance(self.__dict__[key], ConfigObject):
                    continue

                self.__dict__[key] = val
                continue

            logging.warning("Неправильный ключ конфига {}".format(key))

    def _update_from_env(self):
        for key, val in self.__dict__.items():
            if isinstance(val, ConfigObject):
                continue

            env_param = key if not self.CONF_NAME else '_'.join((self.CONF_NAME, key))
            if env_param in os.environ:
                self.__dict__[key] = os.environ[env_param]

    def _assert_and_log(self):
        for key, val in self.__dict__.items():
            if key != 'password':
                log.info('Section %s=%s', key if not self.CONF_NAME else '_'.join((self.CONF_NAME, key)), val)
            if val == ConfigObject.UNDEFINED:
                raise RuntimeError('Param %s_%s required' % (self.CONF_NAME, key))


class RootConfig(ConfigObject):
    CONF_NAME = ''

    # Константы контейнера из Dockerfile
    WORK_DIR = '/usr/local/test_tools_data'
    ENGINE_PORT = 8082
    ENVIRONMENT_CONFIG = os.path.join(os.path.dirname(__file__), 'config_files', 'environment.json')
    CUSTOM_CONFIG = os.path.join(WORK_DIR, 'stand_config.json')
    CATALINA_SH = "catalina.sh"
    CATALINA_LOGS = '/usr/local/tomcat/logs/'

    UNI_TEMPLATES = os.path.join(os.path.dirname(__file__), 'config_files', 'uni')
    UNI_TEMPLATE_POSTGRES = os.path.join(UNI_TEMPLATES, 'postgres_hibernate.properties')
    UNI_TEMPLATE_MSSQL = os.path.join(UNI_TEMPLATES, 'mssql_hibernate.properties')
    UNI_TEMPLATE_CONFIG_DIR = os.path.join(UNI_TEMPLATES, 'config')

    UNI_CONFIG_DIR = os.path.join(WORK_DIR, 'config')
    UNI_VERSION_FILE = os.path.join(UNI_CONFIG_DIR, 'version.txt')
    UNI_CONFIG_DB_FILE = os.path.join(UNI_CONFIG_DIR, 'hibernate.properties')

    UNI_WEBAPP = os.path.join(WORK_DIR, 'webapp')

    UNI_PORT = 8080
    UNI_DEBUG_PORT = 8081

    def __init__(self):
        self.log_level = 'INFO'
        self.db_type = Pgdocker.DB_TYPE
        self.catalina_opts = "-Dapp.install.path={} -Xmx1500m -Djava.awt.headless=true -Dfile.encoding=UTF-8 -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address={}".format(
                RootConfig.WORK_DIR, str(RootConfig.UNI_DEBUG_PORT))

        self.jenkins = JenkinsConfig()
        self.db = DBConfig(Pgdocker.DB_TYPE)

    def make_config(self):
        """
        Готовит итоговый конфиг из всего что может быть определено, в порядке приоритета:
        Переменные среды >  environment.json > умолчания в классе
        :return:
        """

        # Загружаем дефолтные параметры текущего окружения
        with open(RootConfig.ENVIRONMENT_CONFIG, 'rt') as f:
            environment_config = json.load(f)

        # сначала нужно определить тип базы и уровень логирования поэтому первым делом подгружаем корневые параметры
        self._update_from_dict(environment_config[RootConfig.CONF_NAME])
        self._update_from_env()
        # теперь можно сконфигурировать логи
        logging.config.dictConfig(self.default_logging())
        # и после писать лог
        self._assert_and_log()

        # Определяем дефолтные параметры бд (для используемого типа)
        self.db = DBConfig(self.db_type)
        # Определяем параметры окружения для бд
        self.db._update_from_dict(environment_config[self.db_type])

        # Определяем параметры окружения для сборщика
        self.jenkins._update_from_dict(environment_config[JenkinsConfig.CONF_NAME])

        # Подгружаем пользовательские параметры и валидируем
        for conf_obj in (self.db, self.jenkins):
            conf_obj._update_from_env()
            conf_obj._assert_and_log()

        return self

    def default_logging(self):
        return {
            'version': 1,
            'formatters': {
                'main_formatter': {'format': '%(asctime)s %(levelname)s %(name)s: %(message)s'},
            },
            'handlers': {
                'console': {'class': 'logging.StreamHandler', 'formatter': 'main_formatter'},
                'file': {'class': 'logging.FileHandler', 'formatter': 'main_formatter',
                         'filename': os.path.join(RootConfig.WORK_DIR, 'log.txt')},
            },
            'loggers': {
                'tornado.application': {'level': logging.ERROR},
                '[test tools pgdocker]': {},
                '[test tools postgres]': {},
                '[test tools mssql]': {},
                '[test tools main]': {},
                '[test tools jenkins]': {},
                '[test tools config]': {},
            },
            'root': {
                'level': self.log_level,
                'handlers': ['console', 'file']
            },
        }


class DBConfig(ConfigObject):
    CONF_NAME = 'db'

    def __init__(self, db_type=Pgdocker.DB_TYPE):
        if db_type not in (Pgdocker.DB_TYPE, Postgres.DB_TYPE, Mssql.DB_TYPE):
            raise RuntimeError('Unsupported db_type')

        self.ip = ConfigObject.UNDEFINED
        self.port = None
        self.name = ConfigObject.UNDEFINED
        self.user = ConfigObject.UNDEFINED
        self.password = ConfigObject.UNDEFINED
        self.backup_dir = ConfigObject.UNDEFINED
        # стандартный режим запуска uni - если в базе лишние сущности то не стартует,
        # false для баз где есть клиентские сущности
        self.validate_entity_code = True

        # (!) Использование данной опции удалит базу данных во время остановки конйтенера.
        # Не используйте ее если хотите сохранить результаты работы
        self.rm = False

        if db_type == Mssql.DB_TYPE:
            # директория на сервере mssql куда складывать базы (должна существовать)
            self.mssql_db_dir = ConfigObject.UNDEFINED
            self.port = '1433'

        if db_type in (Pgdocker.DB_TYPE, Postgres.DB_TYPE):
            self.backup_dir = os.path.join(RootConfig.WORK_DIR, 'backup')
            self.postgres_ignore_restore_errors = True

        if db_type == Postgres.DB_TYPE:
            self.port = '5432'

        if db_type == Pgdocker.DB_TYPE:
            # адрес докера в сети контейнеров по умолчанию
            self.ip = '172.17.0.1'
            # Без разницы какое имя базы, она одна в контейнере
            self.name = 'uni'
            # Имя контейнера базы данных, если не указано будет создан с рандомным именем
            self.container = None
            # задан в контейнере
            self.user = 'postgres'
            # задается при создании контейнера
            self.password = 'postgres'
            # образ базы данных
            self.pgdocker_image = 'tandemservice/postgres'


class JenkinsConfig(ConfigObject):
    CONF_NAME = 'jenkins'

    def __init__(self):
        self.url = ConfigObject.UNDEFINED
        self.user = ConfigObject.UNDEFINED
        self.password = ConfigObject.UNDEFINED
        self.project = 'product_uni'
        self.branch = None
