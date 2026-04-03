################################################################################
# Copyright 2025 ZT Systems. All Rights Reserved.
# Teams: MFG Test SW, ValTech
################################################################################
"""
ztcopernicus.core.wrappers (formerly ztcopernicus.core.miscellaneous)

Functions and wrappers related to 
"""

__version__ = "1.1.0"

import datetime
import importlib.metadata
import inspect
import os
import sys
from functools import wraps

from ztcopernicus.core import (config, custom_logger, exceptions, plugin_helper, requirements,
                               results)
from ztcopernicus.lib import miscellaneous

# OS/Arch
plat_key = miscellaneous.get_current_os()
arch_key = miscellaneous.get_current_arch()

# Functions that can be ran regardless of OS/Arch
IGNORE_OS_ARCH_CHECK = [
    "get_manifest",
    "get_supported_functions",
    "shutdown",
]

def set_interactive(plugin_object):
    if plugin_object.config["modes"]["noninteractive"]:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")

# endregion


# region FUNCTION/CLASS WRAPPERS

def test_function_args(function_handle, args, kwargs):
    """
    Tests the provided args against the function signature, also against plugin design requirements.
    Raises exceptions on failure.
    """
    sig = inspect.signature(function_handle)
    try:
        sig.bind(*args, **kwargs)
    except TypeError:
        raise exceptions.ErrorMessage(
            f"Arguments do not match function signature. This is the function signature: {str(sig)}")
    # TODO: should we still enforce string arguments?
    for i, arg in enumerate(args):
        if i == 0: #ignore the first argument, which is self
            continue
        if not isinstance(arg, str):
            # raise exceptions.ErrorMessage(f"A non-string argument was provided for positional parameter {i}")
            pass
    for param, arg in kwargs.items():
        if not isinstance(arg, str):
            # raise exceptions.ErrorMessage(f"A non-string argument was provided for parameter {param}")
            pass

def is_api_function(function_name):
    """
    :return (bool): whether the selected function is NOT a helper/private method
    """
    return not function_name.startswith("_")

def api_function_pre_exec(function_handle, args, kwargs):
    """ Things we do before running an API function. Raises exceptions on failure. """
    plugin_object = args[0]
    test_function_args(function_handle, args, kwargs)
    if plugin_object.config["modes"]["dry_run"]:
        raise exceptions.ResultException(results.Result(
            f"Successful dry run of function: {function_handle.__name__}",
            exit_message="dry_run_complete"))

def run_function(function_handle, args, kwargs):
    """
    Runs the function.

    :return: The function's return value
    """
    return function_handle(*args, **kwargs)

def plugin_method_wrapper(func):
    """
    EVERYTHING that we need to wrap an Plugin method with (including non-API/helper methods,
    but not __init__)
    e.g. exception handling (including parameter checks), Results post-processing, etc

    :param func: the outer function
    :return: decorated function
    """

    @wraps(func)
    def inner(*args, **kwargs):
        """
        The function wrapper, which handles the "hidden" keyword arguments (kwargs) before running the actual function.

        Parameters:
            log_result (bool): Optional arg for non-API functions to log their args/result, or for API functions to not
                log their args/result.
        """
        # Track runtime
        start_time = datetime.datetime.now()

        # I maintain and check call_depth to distinguish the top level function call from the rest
        if "call_depth" not in dir(args[0]):
            args[0].call_depth = 0
        args[0].call_depth += 1

        # Keep a function stack to attempt to show which plugins/functions are calling each other.
        if "function_stack" not in dir(args[0]):
            args[0].function_stack = []
        args[0].function_stack.append(func.__name__)

        # When a Plugin object is initialized with the "function_name" param (i.e. the Plugin was initialized in order to call
        # that specific function, as is usually the case), it is expected to treat that as the top-level function.
        is_top_level_function = args[0].config_object.function_name == func.__name__ and args[0].call_depth == 1

        # Handle "hidden" function params
        log_result = kwargs.pop("log_result", None)

        # Log function metadata
        if (is_api_function(func.__name__) and log_result is not False) or log_result is True:
            # Try to determine who is calling the function, where "User" is the user who called the top level function
            if is_top_level_function:
                user = "User"
            elif len(args[0].function_stack) > 1:
                user = f"{args[0].plugin_name}.{args[0].function_stack[-2]}"
            else:
                # TODO: This could be more accurate. We aren't currently able to track the "parent" plugin when one plugin calls
                # another, but we could possibly replace the 'parent_logger' with 'parent_plugin' and then each plugin will know
                # its parent plugin and function_stack.
                user = "Parent plugin"
            args[0].logger.debug(f"{user} is calling function {args[0].plugin_name}.{func.__name__} with args: ({args[1:]}, {kwargs})")

        # Check OS/arch at top-level function
        if args[0].call_depth == 1 and args[0].config_object.function_name not in IGNORE_OS_ARCH_CHECK:
            supported_oss = args[0].requirements.requirements_dict[args[0].plugin_name]["supported_oss"]
            supported_archs = args[0].requirements.requirements_dict[args[0].plugin_name]["supported_archs"]
            if plat_key not in supported_oss:
                raise exceptions.ErrorMessage(f"[{plat_key}] is not one of the supported operating systems: {supported_oss}")
            elif arch_key not in supported_archs:
                raise exceptions.ErrorMessage(f"[{arch_key}] is not one of the supported architectures: {supported_archs}")

        # Attempt to run function
        if not args[0]._api_enable:
            generic_return = results.Result(
                "Plugin API was called after already being shutdown",
                exit_message="framework_used_incorrectly")
        elif is_api_function(func.__name__) and is_top_level_function:
            orig_stdout = sys.stdout
            set_interactive(args[0])
            try:
                api_function_pre_exec(func, args, kwargs)
                generic_return = run_function(func, args, kwargs)
            except exceptions.Error as e:
                generic_return = results.Result(
                    e.error_message,
                    exit_message=e.error_message,
                    exit_code=e.exit_code,
                    next_action_hint="stop")
            except exceptions.ResultException as e:
                generic_return = e.result
            except KeyboardInterrupt:
                # TODO: Should we kill the process here instead? If running plugins in sequeunce,
                # like in Constable/Sparrow, the user will have to press CTRL+C once for each
                # plugin to end it.
                generic_return = results.Result(
                    "Interrupted by CTRL-C or termination signal",
                    exit_message="killed_by_user")
            except BaseException as e:
                generic_return = results.GenericErrorResult(
                    plugin_object=args[0],
                    module_name=__name__,
                    exception=e)

            # Verify that the function returned a Result object
            if not isinstance(generic_return, results.Result):
                args[0].logger.debug(f"WARNING: A Result object was not returned by an API function: [{func.__name__}]!")
                generic_return = results.Result(
                    "Top level API function call did not return a Result object",
                    exit_message="framework_used_incorrectly")

            # Add log paths
            log_full_path = args[0].config["logs"]["file"]
            if (os.path.isfile(log_full_path)
                and os.stat(log_full_path).st_size > 0
                and log_full_path not in generic_return.logs
            ):
                generic_return.logs.append(log_full_path)

            sys.stdout = orig_stdout
            args[0].call_depth = 0
        else:
            generic_return = run_function(func, args, kwargs)

        # Add duration (Result object only)
        if isinstance(generic_return, results.Result):
            generic_return.duration_in_sec = (datetime.datetime.now() - start_time).total_seconds()

        # Log return value
        if (is_api_function(func.__name__) and log_result is not False) or log_result is True:
            log_return = generic_return
            # Only show the result for successful non-top-level function runs
            if isinstance(generic_return, results.Result) and generic_return.exit_code == 0 and not is_top_level_function:
                log_return = miscellaneous.print_beautifully(generic_return.results, to_string=True)
            try:
                args[0].logger.debug(f"{args[0].plugin_name}.{func.__name__} returned: {log_return}")
            except TypeError:
                args[0].logger.error(f"{args[0].plugin_name}.{func.__name__} returned: <Failed to serialize return value>")

        args[0].function_stack.pop(-1)
        return generic_return

    return inner

def init_wrapper(func):
    """
    A way to auto-magically hook pre/post steps into the plugin's __init__ method.
    Remember, __init__ must return None, so do not change the return value.

    :param func: the outer method
    :return: decorated function
    """
    def init_pre_exec(plugin_object, parent_logger=None, noninteractive=None, dry_run=None,
                      log_file=None, log_level=None, bypass_requirements=True,
                      copy_tools=False, show_environ=False, function_name=""):
        """
        Args:
            plugin_object (Plugin): The Plugin object to initialize
            parent_logger (logging.Logger): Parent logger to write to instead of the default plugin logger
            noninteractive (bool): Set to True to suppress all stdout
            dry_run (bool): Set to True to only run the wrapper methods around a plugin function
            log_file (path-like str): Path to log file to use as file handler
            log_level (int): Minimum log level to show in log file
            bypass_requirements (bool): Set to False to run requirements check while initializing the plugin
            copy_tools (bool): Set to True to copy tools from share drive into the ztcopernicus package
            show_environ (bool): Set to True to log os.environ, sys.path, and Config object attributes
            function_name (str): Name of the function that this plugin is being instantiated to run
        """

        # Instantiate the config
        overrides = {}
        if log_level is not None:  # Filter out the None values which imply "use the default"
            overrides["log_level"] = log_level
        if log_file is not None:
            overrides["log_file"] = log_file
        if noninteractive is not None:
            overrides["noninteractive"] = noninteractive
        if dry_run is not None:
            overrides["dry_run"] = dry_run
        plugin_object.config_object = config.Config(
            plugin_object=plugin_object,
            sideband_inputs=overrides,
            function_name=function_name
        )
        plugin_object.config = plugin_object.config_object.config
        plugin_object.plugin_name = plugin_object.config_object.plugin_name
        plugin_object.show_environ = show_environ
        try:
            # component type / parent key under plugin name in golden specs
            plugin_object.component = plugin_helper.get_plugin_component(plugin_object.plugin_name)
        except Exception:
            plugin_object.component = plugin_object.plugin_name

        # Add OS and arch as class vars
        plugin_object.os_type = miscellaneous.get_current_os()
        plugin_object.arch = miscellaneous.get_current_arch()

        # Set whether to copy tools when the Plugin is initialized
        plugin_object.copy_tools = copy_tools

        # Now that we have the config, we know whether to redirect stdout
        plugin_object.orig_stdout = sys.stdout
        set_interactive(plugin_object)

        # Instantiate the logger
        plugin_object.logger = custom_logger.get_my_logger(
            config_object=plugin_object.config_object,
            parent_logger=parent_logger
        )

        # Log some info about this plugin object
        dump_environment(plugin_object, is_entry_point=bool(function_name))

        # Instantiate the requirements object
        plugin_object.requirements = requirements.Requirements(plugin_object)

        # Check requirements
        # TODO: Make this configurable via the CLI and/or config file
        plugin_object.bypass_requirements = bool(bypass_requirements)
        if not plugin_object.bypass_requirements and plugin_object.requirements.check():
            raise exceptions.ErrorMessage("Failed to initialize plugin due to failed requirements check.")

    def init_post_exec(plugin_object):
        sys.stdout = plugin_object.orig_stdout
        plugin_object.call_depth = 0

    @wraps(func)
    def inner(plugin_object, *args, parent_logger=None, noninteractive=False,
              dry_run=False, log_file=None, log_level=None, bypass_requirements=True,
              copy_tools=False, function_name="", **kwargs):
        """
        Inner wrapper
        All explicit keyword args (parent_logger, noninteractive, ..etc) are meant for the
        pre/post exec functions only.
        The other args will be passed thru to the wrapped method.
        """
        if plugin_object.init_wrapper_enable:
            # I only want this wrapper to be effective on the first call to __init__()
            plugin_object.init_wrapper_enable = False
            init_pre_exec(
                plugin_object=plugin_object,
                parent_logger=parent_logger,
                noninteractive=noninteractive,
                dry_run=dry_run,
                log_file=log_file,
                log_level=log_level,
                bypass_requirements=bypass_requirements,
                copy_tools=copy_tools,
                function_name=function_name)
            func(plugin_object, *args, **kwargs)
            init_post_exec(plugin_object)
        else:
            func(plugin_object, *args, **kwargs)

    return inner

def main_class_wrapper():
    """
    Main class wrapper. I expect that the main class of each plugin to have this as the most-outer
    decorator.

    :return: decorated class
    """

    def decorate(cls):
        protected_attributes = ["__del__"]
        parent_cls = inspect.getmro(cls)[1]
        for attr in inspect.getmembers(cls, inspect.isfunction):
            if attr[0] == "__init__":
                setattr(cls, attr[0], init_wrapper(getattr(cls, attr[0])))
                if cls.__doc__ is None:
                    cls.__doc__ = ""
                cls.__doc__ +=\
    """
    Info on the constructor:
    :param dry_run: Optional. Boolean. If true, API functions calls will be tested for correct
                         arguments, but not actually execute.
    :param noninteractive: Optional. Boolean. If true, hides all stdout/stderr, except for the final
                           Result JSON
    :param parent_logger: Optional. If provided, this plugin object will create and log to a child
                          of parent_logger, instead of logging to its own file
    :param log_level: Optional. Name for the logging level to use. Defaults to "INFO".
                      If None, I fall back on the config file value
    :param log_file: Optional. Log file path.
                     If None, I fall back on the config file value
    """
            elif attr[0] in protected_attributes or attr in inspect.getmembers(parent_cls,
                                                                               inspect.isfunction):
                pass
            else:
                # Won't work if we ever have multiple inheritance
                setattr(cls, attr[0], plugin_method_wrapper(getattr(cls, attr[0])))
        return cls
    return decorate

# endregion


# region METADATA

def dump_environment(plugin_object, is_entry_point=True):
    """ Dumps environment and configuration for this plugin (some of it is only useful for debug) """

    config_object = plugin_object.config_object
    plugin_name = config_object.plugin_name
    logger = plugin_object.logger

    if is_entry_point:
        # NOTE: We don't use __version__ from version.py here because the package metadata
        # can differ from that version if using a dev0 version. This is subject to change,
        # but in general the package metadata version will be more accurate.
        logger.debug(f"Using ztcopernicus v{importlib.metadata.version('ztcopernicus')}")

    logger.debug(f"Using {plugin_name} v{config_object.version}")

    if plugin_object.show_environ:
        logger.debug("Python's os.environ used in this run:")
        logger.debug("------")
        for k, v in os.environ.items():
            logger.debug(f"{k}={v}")
        logger.debug("------")

        logger.debug("Python's sys.path used in this run:")
        logger.debug("------")
        for p in sys.path:
            logger.debug(p)
        logger.debug("------")

        logger.debug("Config settings used in this plugin:")
        logger.debug("------")
        logger.debug(config_object.config)
        logger.debug("------")

# endregion
