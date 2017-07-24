import logging
import os
import shutil
import time
import zipfile
import pytz

import jenkinsapi

from config import JenkinsConfig

log = logging.getLogger('[test tools jenkins]')


class Jenkins:
    def __init__(self, jenkins_config):
        # https://jenkinsapi.readthedocs.io/en/latest/build.html
        assert isinstance(jenkins_config, JenkinsConfig)
        self.url = jenkins_config.url
        self.user = jenkins_config.user
        self.password = jenkins_config.password
        self.project = jenkins_config.project
        self.version = jenkins_config.branch

    def build_project(self):
        """
        Запускает job и ждет окончания
        :return:Номер сборки
        """
        log.debug('call build project %s and version %s', self.project, self.version)
        s = jenkinsapi.jenkins.Jenkins(self.url,
                                       username=self.user,
                                       password=self.password)
        job = s[self.project]

        if self.version:
            params = {'Version': self.version}
        else:
            params = None

        build_number = job.get_next_build_number()
        log.info('start build project %s with params %s, build %s', self.project, params, build_number)
        s.build_job(self.project, params)

        # jenkinsapi не считает билд существующим пока не начнется его фактическая сборка, поэтому приходится писать так
        elapsed_time = 0
        while 1:
            if elapsed_time > 600:
                break
            try:
                if not job.get_build(build_number).is_running():
                    break
            except KeyError:
                pass
            log.debug('wait build')
            elapsed_time += 15
            time.sleep(15)

        if not job.get_build(build_number).is_good():
            raise RuntimeError('Last build is incorrect')

        return build_number

    def get_build(self, dir_for_files, build_number=None):
        """
        Скачивает и распаковывает war файл
        :param dir_for_files: Директория для распаковки war
        :param build_number: Номер сборки
        """
        log.info('Get build. Project: %s, directory: %s, build: %s', self.project, dir_for_files, build_number)
        s = jenkinsapi.jenkins.Jenkins(self.url,
                                       username=self.user,
                                       password=self.password)
        job = s[self.project]

        if build_number:
            build = job.get_build(build_number)
        else:
            build = job.get_last_build()
            build_number = build.get_number()

        log.debug('%s build status %s', build.get_number(), build.get_status())

        if not build.is_good():
            raise RuntimeError('Last build of project %s is not SUCCESS' % self.project)

        # очищаем директорию
        shutil.rmtree(dir_for_files)
        os.mkdir(dir_for_files)
        war_file = os.path.join(dir_for_files, 'last_build.war')

        log.debug('start loading build artifact for project %s and build number %s', self.project, build_number)
        for war in build.get_artifacts():
            assert isinstance(war, jenkinsapi.artifact.Artifact)
            log.debug('download %s to %s', war.filename, war_file)

            try:
                war.save(war_file, strict_validation=False)
            # костыль, не знаю почему исключение
            except jenkinsapi.custom_exceptions.ArtifactBroken:
                pass

            if not zipfile.is_zipfile(war_file):
                raise RuntimeError('Cannot unpack build artifact. It is not zip file')

            log.debug('Unpack war')
            f = zipfile.ZipFile(war_file)
            f.extractall(path=dir_for_files)
            f.close()

            build_ts = build.get_timestamp()
            local_datetime_string = build_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Yekaterinburg')) \
                .strftime('%d.%m.%Y %H:%M')
            return '{0} build {1}'.format(local_datetime_string, build_number)
