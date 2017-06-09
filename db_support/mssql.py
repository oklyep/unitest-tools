import logging
import pymssql
import time
from threading import Timer

from db_support.db_tools import DBTools

log = logging.getLogger('[test tools mssql]')


class Mssql(DBTools):
    DB_TYPE = "mssql"

    def __init__(self, db_config):
        super(Mssql, self).__init__(db_config)
        self.db_files_dir = db_config.mssql_db_dir
        self.backup_path = '{}\\{}.bak'.format(db_config.backup_dir, self.name)

    def _run_sql(self, sql, timeout, connect_to_current_db=True, non_query=True, ignore_errors=False):
        log.debug('Run sql. Server %s, timeout %s, query %s', self.addr, timeout, sql)

        if not self.port:
            self.port = 1433
        kw = {'server': self.addr,
              'user': self.user,
              'password': self.password,
              'port': self.port,
              'timeout': timeout}
        if connect_to_current_db:
            kw['database'] = self.name

        with pymssql.connect(**kw) as conn:
            conn.autocommit(True)
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            except pymssql.Error as e:
                if ignore_errors:
                    log.warning(str(e))
                else:
                    raise e
            if not non_query:
                l = cursor.fetchall()
                log.debug('Result: %s', l)
                return l
            else:
                return cursor.rowcount

    def create(self):
        log.info('Create database %s on server %s', self.name, self.addr)
        sql = 'CREATE DATABASE {name} ON (NAME = {name}_Data, FILENAME = \'{path}\{name}.mdf\') ' \
              'LOG ON (NAME = {name}_Log,  FILENAME = \'{path}\{name}.ldf\');'.format(name=self.name,
                                                                                      path=self.db_files_dir)
        self._run_sql(sql, timeout=self.quick_operation_timeout, connect_to_current_db=False)
        self._run_sql('ALTER DATABASE {} SET READ_COMMITTED_SNAPSHOT ON;'.format(self.name),
                      timeout=self.quick_operation_timeout)
        self._run_sql('ALTER DATABASE {} SET ALLOW_SNAPSHOT_ISOLATION ON;'.format(self.name),
                      timeout=self.quick_operation_timeout)

    def backup(self):
        log.info('Backup database %s on server %s', self.name, self.addr)
        sql = 'BACKUP DATABASE {} TO DISK = \'{}\' WITH INIT'.format(self.name,
                                                                     self.backup_path)
        self._run_sql(sql, timeout=self.backup_timeout)

    def has_default_backup(self):
        log.debug("has default backup checking")
        sql = 'RESTORE FILELISTONLY FROM DISK = \'{}\''.format(self.backup_path)
        try:
            self._run_sql(sql, timeout=self.quick_operation_timeout, non_query=False,
                          connect_to_current_db=False)
        except pymssql.DatabaseError:
            log.info("Default backup not found")
            return False

        return True

    def restore(self):
        log.info('Restore database %s on server %s', self.name, self.addr)
        # Сначала узнаем какие файлы содержит бэкапю Возвращает таблицу
        sql = 'RESTORE FILELISTONLY FROM DISK = \'{}\''.format(self.backup_path)
        file_list = self._run_sql(sql, timeout=self.quick_operation_timeout, non_query=False,
                                  connect_to_current_db=False)

        # Формируем скл запрос, который содержит правильные пути до файлов базы данных
        sql_part = []

        for elem in file_list:
            new_filename = '{}\{}_{}.{}'.format(self.db_files_dir,
                                                self.name,
                                                elem[6],  # индекс файла
                                                ('LDF' if elem[2] == 'L' else 'MDF'))  # L или D. Лог или данные

            sql_part.append('MOVE \'{}\' TO \'{}\''.format(elem[0], new_filename))  # логическое имя
        sql = 'RESTORE DATABASE {} FROM DISK = \'{}\' WITH RECOVERY, REPLACE, {};' \
            .format(self.name,
                    self.backup_path,
                    ', '.join(sql_part))
        self._run_sql(sql, self.restore_timeout, connect_to_current_db=False)

        # Изменить логические имена на новое имя базы данных. Если это файл лога, то добавить log, иначе номер файла
        # Нужно для шринка и очистки и чтобы не было одинаковых логических имен, что потенциально может давать глюки
        for elem in file_list:
            current_name = elem[0].lower()
            good_name = '{}_{}'.format(self.name, ('log' if elem[2] == 'L' else elem[6]))
            if current_name != good_name:
                self._run_sql(
                        'ALTER DATABASE {} MODIFY FILE (NAME = \'{}\', NEWNAME = \'{}\')'.format(self.name,
                                                                                                 current_name,
                                                                                                 good_name),
                        timeout=self.quick_operation_timeout)

    def customer_patch(self):
        log.debug('Выполнение sql специфичных для базы ДВФУ (fefu)')
        # Чистим 60+ ГБ
        self._run_sql('use {}; '
                      'truncate table FEFU_RATING_PKG_STUDENT_ROW; '
                      'alter table FEFU_RATING_PKG_STUDENT_ROW drop constraint fk_ratingpackage_96afe8ba; '
                      'truncate table FEFU_SENDING_RATING_PKG; '
                      'ALTER TABLE FEFU_RATING_PKG_STUDENT_ROW ADD CONSTRAINT fk_ratingpackage_96afe8ba '
                      'FOREIGN KEY (RATINGPACKAGE_ID) REFERENCES FEFU_SENDING_RATING_PKG(ID);'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        # И еще 3+ ГБ Nsi до кучи
        self._run_sql('use {}; truncate table FEFUNSILOGROW_T'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)

    def map_user_schema(self, user, schema):
        log.info('Map user %s to schema %s in database %s on server %s', user, schema, self.name, self.addr)
        sql = 'use {db}; CREATE USER {user} FOR LOGIN {user}; ' \
              'ALTER USER {user} WITH DEFAULT_SCHEMA={schema}; ' \
              'exec sp_addrolemember \'db_owner\', \'{user}\';' \
            .format(db=self.name, user=user, schema=schema)
        self._run_sql(sql, timeout=self.quick_operation_timeout, ignore_errors=True)

    def reduce(self):
        log.info('Reduce database %s on server %s', self.name, self.addr)
        # Отключаем лог транзакций
        self._run_sql('ALTER DATABASE {} SET RECOVERY SIMPLE; '.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=False)

        # Сразу уменьшим логи транзакций чтобы создать больше места
        self._run_sql('use {name}; DBCC SHRINKFILE ({name}_log, 1);'.format(name=self.name),
                      timeout=self.middle_operation_timeout, ignore_errors=True)

        # Чистим логи uni. Констреинт будет создан автоматически платформой при запуске
        self._run_sql('use {}; '
                      'truncate table logeventproperty_t; '
                      'alter table logeventproperty_t drop constraint fk_event_logeventproperty; '
                      'truncate table logevent_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=False)

        # Чистим логи nsi если они есть
        self._run_sql('use {}; truncate table nsientitylog_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)

        # Удаляем содержимое таблиц, хранящих печатные формы различных документов, если они есть
        self._run_sql('use {}; truncate table STUDENTEXTRACTTEXTRELATION_T;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table StudentOrderTextRelation_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table stdntothrordrtxtrltn_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table employeeordertextrelation_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table employeeextracttextrelation_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table session_doc_printform_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)
        self._run_sql('use {}; truncate table session_att_bull_printform_t;'.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=True)

        # Удаляем файлы, хранящиеся в базе данных. Mssql пылесос блобов работает в фоне,
        # поэтому удаляем серией маленьких транзакций по 500 файлов

        def timeout_err():
            log.error('Timeout while removing database files. Stop operation')

        t = Timer(self.restore_timeout, timeout_err)
        t.start()
        while not t.finished.is_set():
            if self._run_sql(
                    'use {}; update top(1000) databasefile_t set content_p = null where content_p is not null and '
                    '(filename_p not in (\'platform-variables.less\', \'platform.css\', \'shared.css\') '
                    'or filename_p is null);'.format(self.name),
                    timeout=self.middle_operation_timeout, ignore_errors=True, non_query=True) == 0:
                t.cancel()
                break

        # Ждем пока пылесос подчистит оставшееся, если мы начнем шринкать до этого момента,
        # то пылесосить дальше он будет после шринка, что приведет к тому что база будет ужата не полностью
        size_before = 999999999
        while 1:
            table_row = self._run_sql('sp_spaceused DATABASEFILE_T;'.format(self.name),
                                      ignore_errors=False, non_query=False, timeout=self.quick_operation_timeout)[0]
            # третья строка содержит размер данных таблицы,в виде строки, убираю ' KB' чтобы получить число
            size_at_moment = int(table_row[3][:-3])
            if size_at_moment == size_before:
                break
            else:
                size_before = size_at_moment
                time.sleep(60)

        # Еще раз удаляем лог транзакций после операции update
        self._run_sql('use {name}; DBCC SHRINKFILE ({name}_log, 1);'.format(name=self.name),
                      timeout=self.middle_operation_timeout, ignore_errors=True)

        # Уменьшаем базу, освобождаем место на диске. Оставляем 5% свободного места
        self._run_sql('DBCC SHRINKDATABASE ({}, 5);'.format(self.name),
                      timeout=self.restore_timeout, ignore_errors=False)

        # Еще раз удаляем лог транзакций последний операций
        self._run_sql('use {name}; DBCC SHRINKFILE ({name}_log, 1);'.format(name=self.name),
                      timeout=self.middle_operation_timeout, ignore_errors=True)

        # Включаем полноценный лог транзакций
        self._run_sql('ALTER DATABASE {} SET RECOVERY FULL; '.format(self.name),
                      timeout=self.quick_operation_timeout, ignore_errors=False)

    def set_1_1(self):
        log.info('Set user and password 1:1 in database %s on server %s', self.name, self.addr)
        sql = 'use {}; ' \
              'UPDATE principal_t SET LOGIN_P=\'1\', passwordhash_p=\'c4ca4238a0b923820dcc509a6f75849b\', passwordsalt_p=null ' \
              'where ' \
              '(EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\')  and LOGIN_P=\'1\') or ' \
              '(not EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\') and id=(select top 1 id from PRINCIPAL_T where id in (select PRINCIPAL_ID from ADMIN_T) and ACTIVE_P=1));' \
            .format(self.name)
        self._run_sql(sql, timeout=self.quick_operation_timeout, ignore_errors=True)
