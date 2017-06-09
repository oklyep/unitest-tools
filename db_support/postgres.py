import logging
import os
import subprocess

import magic

from db_support.db_tools import DBTools

log = logging.getLogger('[test tools postgres]')


class Postgres(DBTools):
    DB_TYPE = "postgres"

    def __init__(self, db_config):
        super(Postgres, self).__init__(db_config)
        self.ignore_restore_errors = db_config.postgres_ignore_restore_errors

        if not os.path.exists(db_config.backup_dir):
            os.mkdir(db_config.backup_dir)
        self.backup_path = os.path.join(db_config.backup_dir, 'default.backup')

    def _run_console_command(self, args, timeout, ignore_error=False, stdin=None):
        common = [
            '--host', self.addr,
            '--username', self.user,
        ]
        if self.port:
            common.extend(['--port', str(self.port)])
        common.reverse()
        for elem in common:
            args.insert(1, elem)

        log.debug('Run process with command: %s', ' '.join(args))
        os.putenv('PGPASSWORD', self.password)
        process = subprocess.Popen(args=args,
                                   stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=stdin)
        out, err = process.communicate(timeout=timeout)
        if process.returncode != 0:
            log.debug(' '.join(args))
            # Чтобы ошибки восстановления не засирали лог вывводим первые 1000 символов
            error_text = out.decode() + err.decode()
            if len(error_text) < 1000:
                log.debug(error_text)
            else:
                log.debug('{} \n and another {} symbols'.format(error_text[-1000:], len(error_text) - 1000))
            if not ignore_error:
                raise RuntimeError('Console command for postgresql failed. See log for details')

    def create(self):
        log.info('Create database %s on server %s', self.name, self.addr)
        args = ['psql',
                '--command', 'CREATE DATABASE {0}'.format(self.name),
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout)

    def drop(self):
        log.info('Drop database %s on server %s', self.name, self.addr)
        self._run_console_command(['psql',
                                   '--command', 'DROP DATABASE {0}'.format(self.name),
                                   ], timeout=self.quick_operation_timeout)

    def backup(self):
        log.info('Backup database %s on server %s', self.name, self.addr)
        args = ['pg_dump',
                '--dbname', self.name,
                '--format', 'c',
                '--file', self.backup_path,
                ]
        self._run_console_command(args, self.backup_timeout)

    def has_default_backup(self):
        log.debug("has default backup checking")
        if not os.path.exists(self.backup_path):
            log.info("Default backup not found")
            return False
        return True

    def restore(self):
        log.info('Restore database %s on server %s', self.name, self.addr)
        if not os.path.isdir(self.backup_path) and magic.from_file(self.backup_path, mime=False).find(
                'ASCII text') != -1:
            # Чтобы наследники юзали методы родителя
            Postgres.drop(self)
            Postgres.create(self)
            log.info('Restore plain text backup to database %s on server %s', self.name, self.addr)
            with open(self.backup_path) as f:
                args = ['psql', '--quiet',
                        '--dbname', self.name,
                        ]
                if self.ignore_restore_errors:
                    self._run_console_command(args, self.restore_timeout, ignore_error=True, stdin=f)
                else:
                    self._run_console_command(args, self.restore_timeout, ignore_error=False, stdin=f)

        elif os.path.isdir(self.backup_path) \
                or magic.from_file(self.backup_path, mime=False).find('PostgreSQL') != -1 \
                or magic.from_file(self.backup_path, mime=False).find('POSIX tar archive') != -1:
            Postgres.drop(self)
            Postgres.create(self)
            log.info('Restore pg_dump backup to database %s on server %s', self.name, self.addr)
            args = ['pg_restore',
                    '--no-owner', '--no-privileges',
                    '--dbname', self.name,
                    self.backup_path,
                    ]

            if self.ignore_restore_errors:
                self._run_console_command(args, self.restore_timeout, ignore_error=True)
            else:
                args.insert(-1, '--exit-on-error')
                self._run_console_command(args, self.restore_timeout, ignore_error=False)
        else:
            log.error('File type of backup %s', magic.from_file(self.backup_path, mime=False))
            raise RuntimeError('Wrong postgres backup format')

    def customer_patch(self):
        if self.name.find('pgups') != -1:
            log.info('Выполнение sql специфичных для базы ПГУПС (pgups)')
            args = ['psql',
                    '--dbname', self.name,
                    '--command', 'update app_info_s set value_p=\'unipgups-web\'',
                    ]
            self._run_console_command(args, timeout=self.quick_operation_timeout, ignore_error=True)

    def reduce(self):
        log.info('Reduce database %s on server %s', self.name, self.addr)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'truncate logevent_t cascade;',
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'truncate nsientitylog_t;',
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout, ignore_error=True)

        # Удаляем содержимое таблиц, хранящих печатные формы различных документов, если они есть
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table STUDENTEXTRACTTEXTRELATION_T;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table StudentOrderTextRelation_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table stdntothrordrtxtrltn_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table employeeordertextrelation_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table employeeextracttextrelation_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table session_doc_printform_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)
        self._run_console_command(['psql', '--dbname', self.name,
                                   '--command', 'truncate table session_att_bull_printform_t;'.format(self.name)],
                                  timeout=self.quick_operation_timeout, ignore_error=True)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'update databasefile_t set content_p = null where content_p is not null and '
                             '(filename_p is null '
                             'or filename_p not in (\'platform-variables.less\', \'platform.css\', \'shared.css\'));',
                ]
        self._run_console_command(args, timeout=self.restore_timeout, ignore_error=True)

        args = ['psql',
                '--dbname', self.name,
                '--command', 'vacuum full;',
                ]
        self._run_console_command(args, timeout=self.restore_timeout)

    def set_1_1(self):
        log.info('Set user and password 1:1 in database %s on server %s', self.name, self.addr)
        sql = 'UPDATE principal_t SET LOGIN_P=\'1\', passwordhash_p=\'c4ca4238a0b923820dcc509a6f75849b\', passwordsalt_p=null ' \
              'where ' \
              '(EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\') and LOGIN_P=\'1\') or ' \
              '(not EXISTS (select ID from PRINCIPAL_T where LOGIN_P=\'1\') and id=(select id from PRINCIPAL_T where id in (select PRINCIPAL_ID from ADMIN_T) and ACTIVE_P=true limit 1));'
        args = ['psql',
                '--dbname', self.name,
                '--command', sql,
                ]
        self._run_console_command(args, timeout=self.quick_operation_timeout, ignore_error=True)
