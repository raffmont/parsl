from functools import update_wrapper
from functools import partial
from inspect import signature, Parameter

from parsl.app.bash import BashApp
from parsl.app.errors import wrap_error

import hashlib
import datetime
import uuid
import json
import time

from os import makedirs, path, chmod

class Sandbox(object):
    """
    ...
    """

    SCHEMA = "workflow://"

    UNIQUE_ID_HUMAN = "human"
    UNIQUE_ID_UUID = "uuid"

    def __init__(self, scratch_dir_base):
        self._info = {}
        self._scratch_dir_base = scratch_dir_base
        self._working_dir = None

    @property
    def info(self):
        return self._info

    @info.setter
    def info(self, value):
        self._info = value

    @property
    def scratch_dir_base(self):
        return self._scratch_dir_base

    @scratch_dir_base.setter
    def scratch_dir_base(self, value):
        self._scratch_dir_base = value

    @property
    def working_dir(self):
        return self._working_dir

    @working_dir.setter
    def working_dir(self, value):
        self._working_dir = value

    def get_scratch_name(self, unique_id, label):
        """
        Generates a unique name for the task scratch directory name
        :return: Name of the scratch directory
        :rtype: str
        """
        if Sandbox.UNIQUE_ID_HUMAN in unique_id:
            t = time.time()
            self.unique_id = datetime.datetime.utcfromtimestamp(t).strftime('%Y%m%dZ%H%M%S') + "_" + \
                             label + "_" + \
                             hashlib.md5(str(t).encode('utf-8')).hexdigest()
        elif Sandbox.UNIQUE_ID_UUID in unique_id:
            self.unique_id = uuid.uuid()
        return unique_id

    # create path using mkdirs
    def mkdir_working_dir(self, path_dir):
        """
        Make the working directory
        :param path_dir: Path to the working directory
        :type path_dir: str
        """
        makedirs(path_dir)

    # create the working directory
    def create_working_dir(self, unique_id, label):
        """
        Create the working directory
        """
        if self.working_dir is None:
            # Set a scratch directory as working directory
            self.working_dir = self.scratch_dir_base + "/" + self.get_scratch_name(unique_id, label)

            # Set to remove the scratch directory
            self.remove_scratch_dir = True

        # Create scratch directory
        self.mkdir_working_dir(self.working_dir + "/.dagon")

    def get_how_im_script(self):
        """
        Create the script to get the context where the task will be executed
        :return: Context script
        :rtype: str
        """
        return ""

    # Method to be overrided
    def on_execute(self, script, script_name):
        """
        Execute the task script
        :param script: script content
        :type script: str
        :param script_name: script name
        :type script_name: str
        :return: execution result
        :rtype: dict() with the execution output (str) and code (int)
        """
        # The launcher script name
        script_name = self.working_dir + "/.dagon/" + script_name

        # Create a temporary launcher script
        file = open(script_name, "w")
        file.write(script)
        file.flush()
        file.close()
        chmod(script_name, 0o744)

    def pre_process_command(self, command):
        """
        Preprocess the command resolving the dependencies with other tasks
        :param command:
        :type command: command to be executed by the task
        :return: command preprocessed
        :rtype: str
        """


        # Initialize the script
        header = "#! /bin/bash\n"
        header = header + "# This is the DagOn launcher script\n\n"
        header = header + "code=0\n"
        # Add and execute the howim script

        context_script = header + "cd " + self.working_dir + "/.dagon\n"
        context_script += header + self.get_how_im_script() + "\n\n"

        result = self.on_execute(context_script, "context.sh")  # execute context script
        if result['code']:
            raise Exception(result['message'])
        self.info = json.loads(result['output'])

        ### start the creation of the launcher.sh script
        # Create the header
        header = header + "# Change the current directory to the working directory\n"
        header = header + "cd " + self.working_dir + "\n"
        header = header + "if [ $? -ne 0 ]; then code=1; fi \n\n"
        header = header + "# Start staging in\n\n"

        # Create the body
        body = command

        # Index of the starting position
        pos = 0

        # Forever unless no anymore dagon.Workflow.SCHEMA are present
        while True:

            # Get the position of the next dagon.Workflow.SCHEcdMA
            pos1 = command.find(Sandbox.SCHEMA, pos)

            # Check if there is no dagon.Workflow.SCHEMA
            if pos1 == -1:
                # Exit the forever cycle
                break

            # Find the first occurrence of a whitespace (or if no occurrence means the end of the string)
            pos2 = command.find(" ", pos1)

            # Check if this is the last referenced argument
            if pos2 == -1:
                pos2 = len(command)

            # Extract the parameter string
            if command[pos1 - 1] == "\'":
                pos1 -= 1
            arg = command[pos1:pos2]

            # Remove the dagon.Workflow.SCHEMA label
            arg = arg.replace(Sandbox.SCHEMA, "")

            # Split each argument in elements by the slash
            elements = arg.split("/")

            # Extract the referenced task's workflow name
            if elements[0] == "'":
                workflow_name = " "
            else:
                workflow_name = elements[0]

            # The task name is the first element
            task_name = elements[1]

            # Get the rest of the string as local path
            local_path = arg.replace(workflow_name + "/" + task_name, "")

            # Set the default workflow name if needed
            if workflow_name is None or workflow_name == "":
                workflow_name = self.workflow.name

            # Extract the reference task object
            task = self.workflow.find_task_by_name(workflow_name, task_name)

            # Check if the referenced task is consistent
            if task is not None:
                # Evaluate the destiation path
                dst_path = self.working_dir + "/.dagon/inputs/" + workflow_name + "/" + task_name

                # Create the destination directory
                header = header + "\n\n# Create the destination directory\n"
                header = header + "mkdir -p " + dst_path + "/" + path.dirname(local_path) + "\n"
                header = header + "if [ $? -ne 0 ]; then code=1; fi\n\n"
                # Add the move data command

                #header = header + stager.stage_in(self, task, dst_path, local_path)

                # Change the body of the command
                body = body.replace(SandboxApp.SCHEMA + arg, dst_path + "/" + local_path)
            pos = pos2

        # Invoke the command
        header = header + "\n\n# Invoke the command\n"
        header = header + self.include_command(body)
        header = header + "if [ $? -ne 0 ]; then code=1; fi"
        return header


def remote_side_sandbox_executor(func, *args, **kwargs):
    """Executes the supplied function with *args and **kwargs to get a
    command-line to run, and then run that command-line using bash.
    """
    import os
    import time
    import subprocess
    import logging
    import parsl.app.errors as pe
    from parsl import set_file_logger
    from parsl.utils import get_std_fname_mode

    sandbox = Sandbox("scratch")

    logbase = "/tmp"
    format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    # make this name unique per invocation so that each invocation can
    # log to its own file. It would be better to include the task_id here
    # but that is awkward to wire through at the moment as apps do not
    # have access to that execution context.
    t = time.time()

    logname = __name__ + "." + str(t)
    logger = logging.getLogger(logname)
    set_file_logger(filename='{0}/bashexec.{1}.log'.format(logbase, t), name=logname, level=logging.DEBUG,
                    format_string=format_string)

    func_name = func.__name__

    executable = None

    # Try to run the func to compose the commandline
    try:
        # Execute the func to get the commandline
        executable = func(*args, **kwargs)

    except AttributeError as e:
        if executable is not None:
            raise pe.AppBadFormatting("App formatting failed for app '{}' with AttributeError: {}".format(func_name, e))
        else:
            raise pe.BashAppNoReturn(
                "Bash app '{}' did not return a value, or returned None - with this exception: {}".format(func_name, e))

    except IndexError as e:
        raise pe.AppBadFormatting("App formatting failed for app '{}' with IndexError: {}".format(func_name, e))
    except Exception as e:
        logger.error("Caught exception during formatting of app '{}': {}".format(func_name, e))
        raise e

    logger.debug("Executable: %s", executable)

    # Updating stdout, stderr if values passed at call time.

    def open_std_fd(fdname):
        # fdname is 'stdout' or 'stderr'
        stdfspec = kwargs.get(fdname)  # spec is str name or tuple (name, mode)
        if stdfspec is None:
            return None

        fname, mode = get_std_fname_mode(fdname, stdfspec)
        try:
            if os.path.dirname(fname):
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            fd = open(fname, mode)
        except Exception as e:
            raise pe.BadStdStreamFile(fname, e)
        return fd

    std_out = open_std_fd('stdout')
    std_err = open_std_fd('stderr')
    timeout = kwargs.get('walltime')
    project = kwargs.get('project', "")
    unique_id = kwargs.get('unique_id', "HUMAN")

    sandbox.create_working_dir(unique_id, func_name)

    workflow_schema = "workflow://" + project + "/" + unique_id + "/"

    if std_err is not None:
        print('--> executable follows <--\n{}\n--> end executable <--'.format(executable), file=std_err, flush=True)

    return_value = None
    try:

        cwd = None

        working_directory = "scratch" + os.path.sep + unique_id

        os.makedirs(working_directory)

        cwd = os.getcwd()
        os.chdir(working_directory)
        logger.debug("workflow://schema: %s", workflow_schema)

        # Resolve workflow:// inputs
        for input in kwargs.get('inputs', []):
            if "workflow://" in input:
                print(input)

        proc = subprocess.Popen(executable, stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
        proc.wait(timeout=timeout)

        return_value = {
            'unique_id': unique_id,
            'working_directory': working_directory,
            'workflow_schema': workflow_schema,
            'return_code': proc.returncode
        }

        if cwd is not None:
            os.chdir(cwd)

    except subprocess.TimeoutExpired:
        raise pe.AppTimeout("[{}] App exceeded walltime: {}".format(func_name, timeout))

    except Exception as e:
        raise pe.AppException("[{}] App caught exception with return value: {}"
                              .format(func_name, json.dumps(return_value)), e)

    if proc.returncode != 0:
        raise pe.BashExitFailure(func_name, proc.returncode)

    # TODO : Add support for globs here

    missing = []
    for outputfile in kwargs.get('outputs', []):
        fpath = outputfile.filepath

        if not os.path.exists(fpath):
            missing.extend([outputfile])

    if missing:
        raise pe.MissingOutputs("[{}] Missing outputs".format(func_name), missing)

    return return_value


class SandboxApp(BashApp):

    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None):
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache,
                         ignore_for_cache=ignore_for_cache)
        self.kwargs = {}

        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)

        for s in sig.parameters:
            if sig.parameters[s].default is not Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

        # update_wrapper allows remote_side_bash_executor to masquerade as self.func
        # partial is used to attach the first arg the "func" to the remote_side_bash_executor
        # this is done to avoid passing a function type in the args which parsl.serializer
        # doesn't support
        remote_fn = partial(update_wrapper(remote_side_sandbox_executor, self.func), self.func)
        remote_fn.__name__ = self.func.__name__
        self.wrapped_remote_function = wrap_error(remote_fn)

        # Pre process command
