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
import glob
import logging
import os
import re
import shutil
import sys
import tempfile

import pkg_resources

from apache_beam.internal import pickler
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.dataflow.internal import names
from apache_beam.utils import processes

# Package names for different distributions
BEAM_PACKAGE_NAME = 'apache-beam'

# Standard file names used for staging files.
WORKFLOW_TARBALL_FILE = 'workflow.tar.gz'
REQUIREMENTS_FILE = 'requirements.txt'
EXTRA_PACKAGES_FILE = 'extra_packages.txt'


class FileHelper(object):

  def file_download(self, from_url, to_dir, file_name=None):
    """Downloads a file from a URL/path to the local file directory.

    File_name will be extracted from request if file_name is not provided.
    """
    # TODO(silviuc): We should cache downloads so we do not do it for every job.
    if from_url.startswith('http://') or from_url.startswith('https://'):
      try:
        # We check if the file is actually there because wget returns a file
        # even for a 404 response (file will contain the contents of the 404
        # response).
        response, content = __import__('httplib2').Http().request(from_url)
        if int(response['status']) >= 400:
          raise RuntimeError(
              'Beam SDK not found at %s (response: %s)' % (from_url, response))
        file_name = os.path.join(
            to_dir, file_name) if file_name else re.findall(
                'filename=(.+)', response.headers['content-disposition'])
        with open(file_name, 'w') as f:
          f.write(content)
      except Exception:
        logging.info('Failed to download Beam SDK from %s to %s', from_url,
                     file_name)
        raise
    else:
      """Copy file from file system."""
      file_name = os.path.join(
          to_dir, file_name) if file_name else FileSystems.split(from_url)[1]
      self.file_copy(from_url, file_name)

  def file_copy(self, from_path, to_path):
    """Copy files and directory from from_path to to_path."""
    # Used only for unit tests and integration tests.
    if not os.path.isdir(os.path.dirname(to_path)):
      logging.info(
          'Created folder (since we have not done yet, and any errors '
          'will follow): %s ', os.path.dirname(to_path))
      os.mkdir(os.path.dirname(to_path))
    shutil.copyfile(from_path, to_path)


class Stager(object):

  def __init__(self, file_helper=FileHelper()):
    self.file_helper = file_helper

  def _get_python_executable(self):
    # Allow overriding the python executable to use for downloading and
    # installing dependencies, otherwise use the python executable for
    # the current process.
    python_bin = os.environ.get('BEAM_PYTHON') or sys.executable
    if not python_bin:
      raise ValueError('Could not find Python executable.')
    return python_bin

  def _populate_requirements_cache(self, requirements_file, cache_dir):
    # The 'pip download' command will not download again if it finds the
    # tarball with the proper version already present.
    # It will get the packages downloaded in the order they are presented in
    # the requirements file and will not download package dependencies.
    cmd_args = [
        self._get_python_executable(),
        '-m',
        'pip',
        'download',
        '--dest',
        cache_dir,
        '-r',
        requirements_file,
        # Download from PyPI source distributions.
        '--no-binary',
        ':all:'
    ]
    logging.info('Executing command: %s', cmd_args)
    processes.check_call(cmd_args)

  def _build_setup_package(self, setup_file, temp_dir, build_setup_args=None):
    saved_current_directory = os.getcwd()
    try:
      os.chdir(os.path.dirname(setup_file))
      if build_setup_args is None:
        build_setup_args = [
            self._get_python_executable(),
            os.path.basename(setup_file), 'sdist', '--dist-dir', temp_dir
        ]
      logging.info('Executing command: %s', build_setup_args)
      processes.check_call(build_setup_args)
      output_files = glob.glob(os.path.join(temp_dir, '*.tar.gz'))
      if not output_files:
        raise RuntimeError(
            'File %s not found.' % os.path.join(temp_dir, '*.tar.gz'))
      return output_files[0]
    finally:
      os.chdir(saved_current_directory)

  def _stage_beam_sdk_tarball(self, file_copy, sdk_remote_location, staged_path,
                              temp_dir):
    """Stage a Beam SDK tarball with the appropriate version.

        Args:
          sdk_remote_location: A GCS path to a SDK tarball or a URL from the
            file can be downloaded.
          staged_path: GCS path where the found SDK tarball should be copied.
          temp_dir: path to temporary location where the file should be
            downloaded.

        Raises:
          RuntimeError: If wget on the URL specified returs errors or the file
            cannot be copied from/to GCS.
        """
    if (sdk_remote_location.startswith('http://') or
        sdk_remote_location.startswith('https://')):
      logging.info('Staging Beam SDK tarball from %s to %s',
                   sdk_remote_location, staged_path)
      local_download_file = self.file_helper.file_download(
          sdk_remote_location, temp_dir, 'beam-sdk.tar.gz')
      file_copy(local_download_file, staged_path)
    elif sdk_remote_location.startswith('gs://'):
      # Stage the file to the GCS staging area.
      logging.info('Staging Beam SDK tarball from %s to %s',
                   sdk_remote_location, staged_path)
      file_copy(sdk_remote_location, staged_path)
    elif sdk_remote_location == 'pypi':
      logging.info('Staging the SDK tarball from PyPI to %s', staged_path)
      file_copy(self._download_pypi_sdk_package(temp_dir), staged_path)
    else:
      raise RuntimeError(
          'The --sdk_location option was used with an unsupported '
          'type of location: %s' % sdk_remote_location)

  def _download_pypi_sdk_package(self, temp_dir):
    """Downloads SDK package from PyPI and returns path to local path."""
    package_name = self.get_sdk_package_name()
    try:
      version = pkg_resources.get_distribution(package_name).version
    except pkg_resources.DistributionNotFound:
      raise RuntimeError('Please set --sdk_location command-line option '
                         'or install a valid {} distribution.'
                         .format(package_name))

    # Get a source distribution for the SDK package from PyPI.
    cmd_args = [
        self._get_python_executable(), '-m', 'pip', 'download', '--dest',
        temp_dir,
        '%s==%s' % (package_name, version), '--no-binary', ':all:', '--no-deps'
    ]
    logging.info('Executing command: %s', cmd_args)
    processes.check_call(cmd_args)
    zip_expected = os.path.join(temp_dir, '%s-%s.zip' % (package_name, version))
    if os.path.exists(zip_expected):
      return zip_expected
    tgz_expected = os.path.join(temp_dir,
                                '%s-%s.tar.gz' % (package_name, version))
    if os.path.exists(tgz_expected):
      return tgz_expected
    raise RuntimeError(
        'Failed to download a source distribution for the running SDK. Expected '
        'either %s or %s to be found in the download folder.' % (zip_expected,
                                                                 tgz_expected))

  def _stage_extra_packages(self, extra_packages, staging_location, temp_dir):
    """Stages a list of local extra packages.

        Args:
          extra_packages: Ordered list of local paths to extra packages to be
            staged.
          staging_location: Staging location for the packages.
          temp_dir: Temporary folder where the resource building can happen.
            Caller is responsible for cleaning up this folder after this
            function returns.

        Returns:
          A list of file names (no paths) for the resources staged. All the
          files
          are assumed to be staged in staging_location.

        Raises:
          RuntimeError: If files specified are not found or do not have expected
            name patterns.
        """
    resources = []
    staging_temp_dir = None
    local_packages = []
    for package in extra_packages:
      if not (os.path.basename(package).endswith('.tar') or
              os.path.basename(package).endswith('.tar.gz') or
              os.path.basename(package).endswith('.whl') or
              os.path.basename(package).endswith('.zip')):
        raise RuntimeError(
            'The --extra_package option expects a full path ending with '
            '".tar", ".tar.gz", ".whl" or ".zip" instead of %s' % package)
      if os.path.basename(package).endswith('.whl'):
        logging.warning(
            'The .whl package "%s" is provided in --extra_package. '
            'This functionality is not officially supported. Since wheel '
            'packages are binary distributions, this package must be '
            'binary-compatible with the worker environment (e.g. Python 2.7 '
            'running on an x64 Linux host).')

      if not os.path.isfile(package):
        # Check and download remote files.
        if package.find('://') is not -1:
          if not staging_temp_dir:
            staging_temp_dir = tempfile.mkdtemp(dir=temp_dir)
          logging.info('Downloading extra package: %s locally before staging',
                       package)
          self.file_helper.file_download(package, staging_temp_dir)
        else:
          raise RuntimeError(
              'The file %s cannot be found. It was specified in the '
              '--extra_packages command line option.' % package)
      else:
        local_packages.append(package)

    if staging_temp_dir:
      local_packages.extend([
          FileSystems.join(staging_temp_dir, f)
          for f in os.listdir(staging_temp_dir)
      ])

    for package in local_packages:
      basename = os.path.basename(package)
      staged_path = FileSystems.join(staging_location, basename)
      self.file_helper.file_copy(package, staged_path)
      resources.append(basename)
    # Create a file containing the list of extra packages and stage it.
    # The file is important so that in the worker the packages are installed
    # exactly in the order specified. This approach will avoid extra PyPI
    # requests. For example if package A depends on package B and package A
    # is installed first then the installer will try to satisfy the
    # dependency on B by downloading the package from PyPI. If package B is
    # installed first this is avoided.
    with open(os.path.join(temp_dir, EXTRA_PACKAGES_FILE), 'wt') as f:
      for package in local_packages:
        f.write('%s\n' % os.path.basename(package))
    # Note that the caller of this function is responsible for deleting the
    # temporary folder where all temp files are created, including this one.
    self.file_helper.file_copy(
        os.path.join(temp_dir, EXTRA_PACKAGES_FILE),
        FileSystems.join(staging_location, EXTRA_PACKAGES_FILE))
    resources.append(EXTRA_PACKAGES_FILE)

    return resources

  def stage_job_resources(
      self,
      options,
      build_setup_args=None,
      staging_location=None,
      temp_dir=None,
      populate_requirements_cache=_populate_requirements_cache):
    """For internal use only; no backwards-compatibility guarantees.

        Creates (if needed) and stages job resources to
        options.staging_location.

        Args:
          options: Command line options. More specifically the function will
            expect staging_location, requirements_file, setup_file, and
            save_main_session options to be present.
          build_setup_args: A list of command line arguments used to build a
            setup package. Used only if options.setup_file is not None. Used
            only for testing.
          temp_dir: Temporary folder where the resource building can happen. If
            None then a unique temp directory will be created. Used only for
            testing.
          populate_requirements_cache: Callable for populating the requirements
            cache. Used only for testing.

        Returns:
          A list of file names (no paths) for the resources staged. All the
          files
          are assumed to be staged in options.staging_location.

        Raises:
          RuntimeError: If files specified are not found or error encountered
          while
            trying to create the resources (e.g., build a setup package).
        """

    if staging_location is None:
      raise RuntimeError('The staging_location must be specified.')
    temp_dir = temp_dir or tempfile.mkdtemp()
    resources = []

    setup_options = options.view_as(SetupOptions)

    # Stage a requirements file if present.
    if setup_options.requirements_file is not None:
      if not os.path.isfile(setup_options.requirements_file):
        raise RuntimeError(
            'The file %s cannot be found. It was specified in the '
            '--requirements_file command line option.' %
            setup_options.requirements_file)
      self.file_helper.file_copy(
          setup_options.requirements_file,
          FileSystems.join(staging_location, REQUIREMENTS_FILE))
      resources.append(REQUIREMENTS_FILE)
      requirements_cache_path = (
          os.path.join(tempfile.gettempdir(), 'dataflow-requirements-cache')
          if setup_options.requirements_cache is None else
          setup_options.requirements_cache)
      # Populate cache with packages from requirements and stage the files
      # in the cache.
      if not os.path.exists(requirements_cache_path):
        os.makedirs(requirements_cache_path)
      populate_requirements_cache(setup_options.requirements_file,
                                  requirements_cache_path)
      for pkg in glob.glob(os.path.join(requirements_cache_path, '*')):
        self.file_helper.file_copy(
            pkg, FileSystems.join(staging_location, os.path.basename(pkg)))
        resources.append(os.path.basename(pkg))

    # Handle a setup file if present.
    # We will build the setup package locally and then copy it to the staging
    # location because the staging location is a GCS path and the file cannot be
    # created directly there.
    if setup_options.setup_file is not None:
      if not os.path.isfile(setup_options.setup_file):
        raise RuntimeError(
            'The file %s cannot be found. It was specified in the '
            '--setup_file command line option.' % setup_options.setup_file)
      if os.path.basename(setup_options.setup_file) != 'setup.py':
        raise RuntimeError(
            'The --setup_file option expects the full path to a file named '
            'setup.py instead of %s' % setup_options.setup_file)
      tarball_file = self._build_setup_package(setup_options.setup_file,
                                               temp_dir, build_setup_args)
      self.file_helper.file_copy(
          tarball_file, FileSystems.join(staging_location,
                                         WORKFLOW_TARBALL_FILE))
      resources.append(WORKFLOW_TARBALL_FILE)

    # Handle extra local packages that should be staged.
    if setup_options.extra_packages is not None:
      resources.extend(
          self._stage_extra_packages(
              setup_options.extra_packages, staging_location,
              temp_dir=temp_dir))

    # Pickle the main session if requested.
    # We will create the pickled main session locally and then copy it to the
    # staging location because the staging location is a GCS path and the file
    # cannot be created directly there.
    if setup_options.save_main_session:
      pickled_session_file = os.path.join(temp_dir,
                                          names.PICKLED_MAIN_SESSION_FILE)
      pickler.dump_session(pickled_session_file)
      self.file_helper.file_copy(
          pickled_session_file,
          FileSystems.join(staging_location, names.PICKLED_MAIN_SESSION_FILE))
      resources.append(names.PICKLED_MAIN_SESSION_FILE)

    if hasattr(setup_options, 'sdk_location'):
      if setup_options.sdk_location == 'default':
        stage_tarball_from_remote_location = True
      elif (setup_options.sdk_location.startswith('gs://') or
            setup_options.sdk_location.startswith('http://') or
            setup_options.sdk_location.startswith('https://')):
        stage_tarball_from_remote_location = True
      else:
        stage_tarball_from_remote_location = False

      staged_path = FileSystems.join(staging_location,
                                     names.DATAFLOW_SDK_TARBALL_FILE)
      if stage_tarball_from_remote_location:
        # If --sdk_location is not specified then the appropriate package
        # will be obtained from PyPI (https://pypi.python.org) based on the
        # version of the currently running SDK. If the option is
        # present then no version matching is made and the exact URL or path
        # is expected.
        #
        # Unit tests running in the 'python setup.py test' context will
        # not have the sdk_location attribute present and therefore we
        # will not stage a tarball.
        if setup_options.sdk_location == 'default':
          sdk_remote_location = 'pypi'
        else:
          sdk_remote_location = setup_options.sdk_location
        self._stage_beam_sdk_tarball(self.file_helper.file_copy,
                                     sdk_remote_location, staged_path, temp_dir)
        resources.append(names.DATAFLOW_SDK_TARBALL_FILE)
      else:
        # Check if we have a local Beam SDK tarball present. This branch is
        # used by tests running with the SDK built at head.
        if setup_options.sdk_location == 'default':
          module_path = os.path.abspath(__file__)
          sdk_path = os.path.join(
              os.path.dirname(module_path), '..', '..', '..',
              names.DATAFLOW_SDK_TARBALL_FILE)
        elif os.path.isdir(setup_options.sdk_location):
          sdk_path = os.path.join(setup_options.sdk_location,
                                  names.DATAFLOW_SDK_TARBALL_FILE)
        else:
          sdk_path = setup_options.sdk_location
        if os.path.isfile(sdk_path):
          logging.info('Copying Beam SDK "%s" to staging location.', sdk_path)
          self.file_helper.file_copy(sdk_path, staged_path)
          resources.append(names.DATAFLOW_SDK_TARBALL_FILE)
        else:
          if setup_options.sdk_location == 'default':
            raise RuntimeError('Cannot find default Beam SDK tar file "%s"',
                               sdk_path)
          elif not setup_options.sdk_location:
            logging.info('Beam SDK will not be staged since --sdk_location '
                         'is empty.')
          else:
            raise RuntimeError(
                'The file "%s" cannot be found. Its location was specified by '
                'the --sdk_location command-line option.' % sdk_path)

    # Delete all temp files created while staging job resources.
    shutil.rmtree(temp_dir)
    return resources

  def get_sdk_package_name(self):
    """For internal use only; no backwards-compatibility guarantees.
      
       Returns the PyPI package name to be staged to Google Cloud Dataflow."""
    return BEAM_PACKAGE_NAME
