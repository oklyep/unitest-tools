class DBTools(object):
    DB_TYPE = 'abstract_database'

    def __init__(self, db_config):
        self.addr = db_config.ip
        self.port = db_config.port
        self.name = db_config.name
        self.user = db_config.user
        self.password = db_config.password

        self.backup_timeout = 3600
        self.restore_timeout = 10800
        self.quick_operation_timeout = 120
        self.middle_operation_timeout = 1200

    def create(self):
        raise NotImplementedError

    def restore(self):
        raise NotImplementedError

    def backup(self):
        raise NotImplementedError

    def has_default_backup(self):
        raise NotImplementedError

    def reduce(self):
        """
        Удалить из базы бОльшую часть блобов, почистить все журналы, сжать базу
        """
        raise NotImplementedError

    def set_1_1(self):
        raise NotImplementedError

    def customer_patch(self):
        """
        Костыль. Ищет в названии базы имя клиента и делает запросы помогающие нам запуститься на этой базе
        """
        raise NotImplementedError

    def drop(self):
        raise NotImplementedError
