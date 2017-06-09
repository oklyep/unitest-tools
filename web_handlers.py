import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.web import RequestHandler

from engine import Engine

log = logging.getLogger('[test tools main]')


class ActionHandler(RequestHandler):
    ENGINE_STATUS = 'engine_status'
    LONG_ACTIONS = ('update', 'reduce', 'backup', 'restore', 'build_and_update',
                    'new_stand', 'new_db', 'drop_db')
    CHECK_UNI_ACTION = 'check_uni'
    # использую внутреннюю очередь tpe чтобы в любой момент времени выполнялась только одна длинная задача
    # т. е. один поток на выполнение длинных задач. Нельзя выполнять параллельно
    TPE = ThreadPoolExecutor(max_workers=1)

    @gen.coroutine
    def _check_uni(self):
        log.info('Check uni')
        engine = self.application.engine
        assert isinstance(engine, Engine)
        cl = AsyncHTTPClient()
        # если работают миграции то время запуска может достигать 15 минут
        deadline = time.time() + 900
        # Если томкат остановлен, значит уни точно недоступен
        while engine.tomcat.poll() is None and time.time() < deadline:
            try:
                yield cl.fetch('http://localhost:{0}/'.format(engine.config.UNI_PORT), request_timeout=900)
                log.info('Uni is available')
                self.finish({'status': 'ok', 'details': 'Uni is available'})
                return
            except (ConnectionError, HTTPError):
                yield gen.sleep(5)

        self.set_status(400, 'Uni is not available')
        self.finish({'status': 'fail', 'error': 'Uni is not available'})

    @gen.coroutine
    def get(self, action):
        """
        Помещает "длинные" задачи в очередь задач. Редирект на главную страницу, которая отобразит текущий статус

        ?sync=1 -> html response только после завершения запроса. Если в очереди много задач будет ждать их
        полного выполения. Вернет стасус выполнения задачи после завершения.
        """
        if action == self.CHECK_UNI_ACTION:
            yield self._check_uni()
            return

        if action in self.LONG_ACTIONS:
            log.info('New task: %s', action)
            sync = self.get_argument('sync', False)
            if sync:
                try:
                    yield self.TPE.submit(getattr(self.application.engine, action))
                    self.finish({'status': 'ok'})
                except Exception as e:
                    log.exception(e)
                    self.set_status(400, 'Error while test tools action')
                    self.finish({'status': 'fail', 'error': str(e)})
                return

            if not sync:
                self.TPE.submit(self.application.engine.log_exceptions, getattr(self.application.engine, action))
                time.sleep(0.5)  # чтобы задача успела начаться, и показать ее статус
                self.redirect('/')
                return

        if action == self.ENGINE_STATUS:
            self.finish(getattr(self.application.engine, action)())
            return

        self.set_status(404, 'invalid action')
        self.finish({'status': 'not found', 'error': 'invalid action'})


class MainPageHandler(RequestHandler):
    with open(os.path.join(os.path.dirname(__file__), 'html', 'main_page.html')) as f:
        HTML_TEMPLATE = f.read()

    def get(self):
        engine = self.application.engine
        assert isinstance(engine, Engine)
        self.finish(self.HTML_TEMPLATE.format(project=engine.config.jenkins.project,
                                              branch=engine.jenkins.version or 'Нет',
                                              db_type=engine.db.DB_TYPE,
                                              db_addr=engine.db.addr,
                                              db_name=engine.db.name,
                                              db_port=engine.db.port or 'Не задан',
                                              active_task=engine.active_task or 'Нет',
                                              last_task=engine.last_task or 'Неизвестно',
                                              last_error=engine.last_error or 'Нет'))


class AdminPageHandler(RequestHandler):
    with open(os.path.join(os.path.dirname(__file__), 'html', 'admin_page.html')) as f:
        HTML_TEMPLATE = f.read()

    def get(self):
        self.finish(self.HTML_TEMPLATE)
