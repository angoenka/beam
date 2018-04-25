#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for the setup module."""

import logging
import os
import shutil
import tempfile
import unittest

from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.dataflow.internal import stager
from apache_beam.runners.dataflow.internal.stager import Stager


class SetupTest(unittest.TestCase):

  def setUp(self):
    self._temp_dir = None
    self.stager = Stager()

  def tearDown(self):
    self.stager = None
    if self._temp_dir:
      shutil.rmtree(self._temp_dir)

  def make_temp_dir(self):
    if self._temp_dir is None:
      self._temp_dir = tempfile.mkdtemp()
    return tempfile.mkdtemp(dir=self._temp_dir)

  @staticmethod
  def update_options(options):
    setup_options = options.view_as(SetupOptions)
    setup_options.sdk_location = ''

  @staticmethod
  def create_temp_file(path, contents):
    with open(path, 'w') as f:
      f.write(contents)
      return f.name

  def populate_requirements_cache(self, requirements_file, cache_dir):
    _ = requirements_file
    self.create_temp_file(os.path.join(cache_dir, 'abc.txt'), 'nothing')
    self.create_temp_file(os.path.join(cache_dir, 'def.txt'), 'nothing')

  def asserting_file_copy(self, expected_from_path, expected_to_dir):

    def file_copy(from_path, to_path):
      if not from_path.endswith(names.PICKLED_MAIN_SESSION_FILE):
        self.assertEqual(expected_from_path, from_path)
        self.assertEqual(
            FileSystems.join(expected_to_dir, names.DATAFLOW_SDK_TARBALL_FILE),
            to_path)
      shutil.copyfile(from_path, to_path)

    return file_copy

  def test_no_staging_location(self):
    with self.assertRaises(RuntimeError) as cm:
      self.stager.stage_job_resources(PipelineOptions())
    self.assertEqual('The staging_location must be specified.',
                     cm.exception.args[0])

  def test_no_main_session(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()

    options.view_as(SetupOptions).save_main_session = False
    self.update_options(options)

    self.assertEqual([],
                     self.stager.stage_job_resources(
                         options, staging_location=staging_dir))

  def test_with_main_session(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()

    options.view_as(SetupOptions).save_main_session = True
    self.update_options(options)

    self.assertEqual([names.PICKLED_MAIN_SESSION_FILE],
                     self.stager.stage_job_resources(
                         options, staging_location=staging_dir))
    self.assertTrue(
        os.path.isfile(
            os.path.join(staging_dir, names.PICKLED_MAIN_SESSION_FILE)))

  def test_default_resources(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()
    self.update_options(options)

    self.assertEqual([],
                     self.stager.stage_job_resources(
                         options, staging_location=staging_dir))

  def test_with_requirements_file(self):
    staging_dir = self.make_temp_dir()
    requirements_cache_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).requirements_cache = requirements_cache_dir
    options.view_as(SetupOptions).requirements_file = os.path.join(
        source_dir, stager.REQUIREMENTS_FILE)
    self.create_temp_file(
        os.path.join(source_dir, stager.REQUIREMENTS_FILE), 'nothing')
    self.assertEqual(
        sorted([stager.REQUIREMENTS_FILE, 'abc.txt', 'def.txt']),
        sorted(
            self.stager.stage_job_resources(
                options,
                staging_location=staging_dir,
                populate_requirements_cache=self.populate_requirements_cache)))
    self.assertTrue(
        os.path.isfile(os.path.join(staging_dir, stager.REQUIREMENTS_FILE)))
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'abc.txt')))
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'def.txt')))

  def test_requirements_file_not_present(self):
    staging_dir = self.make_temp_dir()
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).requirements_file = 'nosuchfile'
      self.stager.stage_job_resources(
          options,
          staging_location=staging_dir,
          populate_requirements_cache=self.populate_requirements_cache)
    self.assertEqual(
        cm.exception.args[0],
        'The file %s cannot be found. It was specified in the '
        '--requirements_file command line option.' % 'nosuchfile')

  def test_with_requirements_file_and_cache(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).requirements_file = os.path.join(
        source_dir, stager.REQUIREMENTS_FILE)
    options.view_as(SetupOptions).requirements_cache = self.make_temp_dir()
    self.create_temp_file(
        os.path.join(source_dir, stager.REQUIREMENTS_FILE), 'nothing')
    self.assertEqual(
        sorted([stager.REQUIREMENTS_FILE, 'abc.txt', 'def.txt']),
        sorted(
            self.stager.stage_job_resources(
                options,
                staging_location=staging_dir,
                populate_requirements_cache=self.populate_requirements_cache)))
    self.assertTrue(
        os.path.isfile(os.path.join(staging_dir, stager.REQUIREMENTS_FILE)))
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'abc.txt')))
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'def.txt')))

  def test_with_setup_file(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'setup.py'), 'notused')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).setup_file = os.path.join(
        source_dir, 'setup.py')

    self.assertEqual(
        [stager.WORKFLOW_TARBALL_FILE],
        self.stager.stage_job_resources(
            options,
            staging_location=staging_dir,
            # We replace the build setup command because a realistic one would
            # require the setuptools package to be installed. Note that we can't
            # use "touch" here to create the expected output tarball file, since
            # touch is not available on Windows, so we invoke python to produce
            # equivalent behavior.
            build_setup_args=[
                'python', '-c', 'open(__import__("sys").argv[1], "a")',
                os.path.join(source_dir, stager.WORKFLOW_TARBALL_FILE)
            ],
            temp_dir=source_dir))
    self.assertTrue(
        os.path.isfile(os.path.join(staging_dir, stager.WORKFLOW_TARBALL_FILE)))

  def test_setup_file_not_present(self):
    staging_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).setup_file = 'nosuchfile'

    with self.assertRaises(RuntimeError) as cm:
      self.stager.stage_job_resources(options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The file %s cannot be found. It was specified in the '
        '--setup_file command line option.' % 'nosuchfile')

  def test_setup_file_not_named_setup_dot_py(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).setup_file = (
        os.path.join(source_dir, 'xyz-setup.py'))

    self.create_temp_file(os.path.join(source_dir, 'xyz-setup.py'), 'notused')
    with self.assertRaises(RuntimeError) as cm:
      self.stager.stage_job_resources(options, staging_location=staging_dir)
    self.assertTrue(cm.exception.args[0].startswith(
        'The --setup_file option expects the full path to a file named '
        'setup.py instead of '))

  def test_sdk_location_default(self):
    staging_dir = self.make_temp_dir()
    expected_from_path = os.path.join(staging_dir, 'sdk-tarball')

    def pypi_download(_):
      tarball_path = os.path.join(staging_dir, 'sdk-tarball')
      with open(tarball_path, 'w') as f:
        f.write('Some contents.')
      return tarball_path

    file_helper = stager.FileHelper()
    file_helper.file_copy = self.asserting_file_copy(expected_from_path,
                                                     staging_dir)
    self.stager = Stager(file_helper=file_helper)
    self.stager._download_pypi_sdk_package = pypi_download
    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = 'default'

    self.assertEqual([names.DATAFLOW_SDK_TARBALL_FILE],
                     self.stager.stage_job_resources(
                         options, staging_location=staging_dir))

  def test_sdk_location_local(self):
    staging_dir = self.make_temp_dir()
    sdk_location = self.make_temp_dir()
    self.create_temp_file(
        os.path.join(sdk_location, names.DATAFLOW_SDK_TARBALL_FILE), 'contents')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    self.assertEqual([names.DATAFLOW_SDK_TARBALL_FILE],
                     self.stager.stage_job_resources(
                         options, staging_location=staging_dir))
    tarball_path = os.path.join(staging_dir, names.DATAFLOW_SDK_TARBALL_FILE)
    with open(tarball_path) as f:
      self.assertEqual(f.read(), 'contents')

  def test_sdk_location_local_not_present(self):
    staging_dir = self.make_temp_dir()
    sdk_location = 'nosuchdir'
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).sdk_location = sdk_location

      self.stager.stage_job_resources(options, staging_location=staging_dir)
    self.assertEqual(
        'The file "%s" cannot be found. Its '
        'location was specified by the --sdk_location command-line option.' %
        sdk_location, cm.exception.args[0])

  def test_with_extra_packages(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'abc.tar.gz'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'xyz.tar.gz'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'xyz2.tar'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'whl.whl'), 'nothing')
    self.create_temp_file(
        os.path.join(source_dir, stager.EXTRA_PACKAGES_FILE), 'nothing')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).extra_packages = [
        os.path.join(source_dir, 'abc.tar.gz'),
        os.path.join(source_dir, 'xyz.tar.gz'),
        os.path.join(source_dir, 'xyz2.tar'),
        os.path.join(source_dir, 'whl.whl'),
        'hdfs://my-gcs-bucket/gcs.tar.gz'
    ]

    gcs_copied_files = []

    def file_download(from_path, to_path):
      if from_path.find('://') is not -1:
        gcs_copied_files.append(from_path)
        _, from_name = os.path.split(from_path)
        if os.path.isdir(to_path):
          to_path = os.path.join(to_path, from_name)
        self.create_temp_file(to_path, 'nothing')
        logging.info('Fake copied Remote file: %s to %s', from_path, to_path)
      else:
        shutil.copyfile(from_path, to_path)

    file_helper = stager.FileHelper()
    file_helper.file_download = file_download
    self.stager = Stager(file_helper=file_helper)
    self.assertEqual([
        'abc.tar.gz', 'xyz.tar.gz', 'xyz2.tar', 'whl.whl', 'gcs.tar.gz',
        stager.EXTRA_PACKAGES_FILE
    ], self.stager.stage_job_resources(options, staging_location=staging_dir))
    with open(os.path.join(staging_dir, stager.EXTRA_PACKAGES_FILE)) as f:
      self.assertEqual([
          'abc.tar.gz\n', 'xyz.tar.gz\n', 'xyz2.tar\n', 'whl.whl\n',
          'gcs.tar.gz\n'
      ], f.readlines())
    self.assertEqual(['hdfs://my-gcs-bucket/gcs.tar.gz'], gcs_copied_files)

  def test_with_extra_packages_missing_files(self):
    staging_dir = self.make_temp_dir()
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).extra_packages = ['nosuchfile.tar.gz']

      self.stager.stage_job_resources(options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The file %s cannot be found. It was specified in the '
        '--extra_packages command line option.' % 'nosuchfile.tar.gz')

  def test_with_extra_packages_invalid_file_name(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'abc.tgz'), 'nothing')
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).extra_packages = [
          os.path.join(source_dir, 'abc.tgz')
      ]
      self.stager.stage_job_resources(options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The --extra_package option expects a full path ending with '
        '".tar", ".tar.gz", ".whl" or ".zip" '
        'instead of %s' % os.path.join(source_dir, 'abc.tgz'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
