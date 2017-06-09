#!/usr/bin/env python3

import logging
import logging.config
import signal

from tornado.ioloop import IOLoop
from tornado.web import Application

from config import RootConfig
from engine import Engine
from web_handlers import ActionHandler, MainPageHandler, AdminPageHandler


def main():
    conf = RootConfig()
    logging.config.dictConfig(conf.default_logging())
    conf.make_config()

    application = Application([
        (r'/', MainPageHandler),
        (r'/admin/*', AdminPageHandler),
        (r'/(.*)', ActionHandler),
    ])

    engine = Engine(conf)
    application.engine = engine

    def exit_handler(signum, frame):
        engine._exit()
        IOLoop.instance().stop()

    # Обработка сигнала завершения контейнера и завершения с консоли
    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGINT, exit_handler)

    application.listen(conf.ENGINE_PORT)
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
