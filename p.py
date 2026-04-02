"""
AnalysisSupport
"""

import json
import os
import re

from ztcopernicus import miscellaneous, plugin_helper

from .lib import utils
from .lib.error_codes import *
from .lib.interface import PluginPackageInterface
from .lib.support_base import SupportBase

# Const analysis types
AnalysisTypes = {
    "assembly": {
        "classifications": ["HW", "SW", "Assembly"],
    },
    "prezc": {
        "classifications": ["HW", "FW", "FRU", "ZC", "SW", "PreZC"],
    },
    "rebootzc": {
        "classifications": ["HW", "FW", "FRU", "ZC", "SW", "RebootZC"],
    },
    "finalzc": {
        "classifications": ["HW", "FW", "FRU", "ZC", "SW", "FinalZC"],
    },
    "rackzc": {
        "classifications": ["HW", "FW", "FRU", "ZC", "SW", "RackZC"],
    },
    "cerberus_activation": {
        "classifications": ["HW", "FW", "FRU", "ZC", "SW", "CerberusActivation"],
    },
    "labzc": {
        "classifications": ["HW", "FW", "FRU", "ZC", "SW", "LabZC"],
    }
}

# If the type is not provided or not found in the AnalysisTypes
# it should use this as a backup default value in .get() method
DefaultType = {
    "classifications": ["HW", "FW", "ZC", "FRU", "SW"]
}

# Keys that are required to be in an analysis node
# TODO: Use pydantic modeling to create an "AnalysisNode" data type
ExpectedKeys = [
    "Status",
    "Golden Spec Value",
    "Inventoried Value",
    "Class",
]

def run_analysis(
    analysis_type: str = None,

    # Generic args for creating the PluginPackageInterface
    logger=None,
    plugin_manifest_path: str = None,
    golden_spec_path: str = None,
    result_file_path: str = None,
    override_components: list = None,

    # You can also create the PluginPackageInterface object yourself and pass it in
    plugin_interface_obj: PluginPackageInterface = None,
):
    """
    Runs Analysis Support module

    :param str analysis_type:
    :param logging.Logger logger:
    :param str plugin_manifest_path:
    :param str golden_spec_path:
    :param str result_file_path:
    :param list override_components:
    :param PluginPackageInterface plugin_interface_obj:
    :return tuple: result dict, exit code
    """
    utils.print_header("Analysis")
    if not plugin_interface_obj:
        plugin_interface_obj = PluginPackageInterface(
            logger=logger,
            plugin_manifest_path=plugin_manifest_path,
            golden_spec_path=golden_spec_path,
            override_components=override_components,
            check_golden_spec_data=True,
        )
        if not plugin_interface_obj.instantiated:
            return {}, 1
    support = AnalysisSupport(
        analysis_type=analysis_type,
        logger=logger,
        result_file_path=result_file_path,
        plugin_interface_obj=plugin_interface_obj,
    )
    return support.run()

def find_all_dictionaries(node, key="", values=[], parent=[]):
    """
    Recursively finds nested dictionaries by key-value

    Args:
        node (dict): Dict to recursively search
        key (str): Key to search for
        values (list): List of desired values for the search key
        parent (list): List of parent keys that have been traversed

    Returns:
        (dict): Yield of nodes that match the search parameters
    """
    if isinstance(node, list):
        for i in node:
            for x in find_all_dictionaries(i, key, values, parent):
                yield x
    elif isinstance(node, dict):
        if key in node and node[key] in values:
            key_name = str(parent[0])
            yield {key_name: node}
        for k, j in node.items():
            for x in find_all_dictionaries(j, key, values, parent + [k]):
                yield x

def find_keys(node, kv):
    """
    Recursively looks through objects and lists to find specified key-value

    Args:
        node (str): The object or list to search through
        kv (str|list): The key value to look for

    Returns:
        (str): Yield of keys matching kv
    """
    # Recursively go through type lists
    if isinstance(node, list):
        for i in node:
            for x in find_keys(i, kv):
                yield x

    # Recursively go through dicts
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in find_keys(j, kv):
                yield x

class AnalysisSupport(SupportBase):
    """AnalysisSupport module for running analysis.

    Args:
        plugin_helper_obj: PluginHelper object
        analysis_type (str): Type of zconform that this analysis should use for classifications
        result_file_path (str): Override result file path
    """
    def __init__(self, plugin_interface_obj, analysis_type=None, logger=None, result_file_path=None):
        super().__init__("analysis", result_file_path)
        self.plugin_interface: PluginPackageInterface = plugin_interface_obj
        if not logger:
            self.logger = self.plugin_interface.logger
        else:
            self.logger = logger
        self.analysis_type = analysis_type
        self.total_failures = 0
        self.total_analyzed = 0
        self.total_bypass = 0
        self.total_errors = 0
        self.result_table.update_headers(["Component", "Failure Item", "Expected", "Actual"])

    def _generate_generic_failure(self, plugin, failure_item, failure_message, classification="SW"):
        """
        Creates a generic failure for the result dict if something fails before it gets to run

        Args:
            plugin (str): plugin name
            failure_item (str): Name of failure item
            failure_message (str): Failure message to display for inventoried value
        """
        self.result_table.add_row([plugin_helper.get_component_type(plugin),
                                  failure_item,
                                  "N/A",
                                  failure_message])
        self.results[plugin] = {
            "results": {
                failure_item: {
                    "Status": False,
                    "Golden Spec Value": "N/A",
                    "Inventoried Value": failure_message,
                    "Class": classification
                }
            },
            "exit_code": ANALYSIS_ERROR,
            "exit_message": failure_message,
            "next_action_hint": "stop"
        }

    def check_analysis_overrides(self, plugin_name, plugin_analysis_results, analysis_type=None):
        """
        Checks for analysis overrides/bypass from the plugin manifest

        TODO: Handle structural errors here and/or add validator to Releases pipeline
        """
        plugin_info = self.plugin_interface.get_plugins()[plugin_name]

        if "analysis_overrides" not in plugin_info:
            return {}

        self.logger.debug("Found 'analysis_overrides' in plugin manifest. Attempting to post-process plugin analysis results...")

        override_result = {}
        override_list = plugin_info["analysis_overrides"]
        for override in override_list:
            self.logger.debug(f"Parsing override parameters: {json.dumps(override, indent=4)}")

            # Get key/regex to look for in result
            if "key" in override:
                key_to_check = override["key"]
            elif "key_regex" in override:
                key_to_check = override["key_regex"]
            else:
                self.logger.debug("'key' or 'key_regex' not found in override parameters, not proceeding.")
                continue

            # Check for ID (SN/JO/SS/SR) and stage overrides
            do_override = False
            if "id" in override and "stage" in override:
                do_override = self.serial_number in override["id"] or self.job_order in override["id"] \
                              or self.ss_code in override["id"] or self.sr_code in override["id"] \
                              and (not analysis_type or analysis_type in override["stage"])
            elif "id" in override and "stage" not in override:
                do_override = self.serial_number in override["id"] or self.job_order in override["id"] \
                              or self.ss_code in override["id"] or self.sr_code in override["id"]
            elif "id" not in override and "stage" in override:
                do_override = not analysis_type or analysis_type in override["stage"]
            if not do_override:
                self.logger.debug("'stage' or 'id' parameters were not found; override will be applied to all stages at the model level")

            # Find desired analysis node to modify
            override_template = {}
            if "key" in override and key_to_check in plugin_analysis_results:
                override_template[key_to_check] = plugin_analysis_results[key_to_check]
            elif "key_regex" in override:
                for key in plugin_analysis_results:
                    if re.match(key_to_check, key):
                        override_template[key] = plugin_analysis_results[key]
            if not override_template:
                self.logger.debug(f"Key(s) matching [{key_to_check}] were requested to be overridden but could not be found.")
                continue

            # Update with overrides
            if "override" not in override:
                self.logger.debug(f"'override' parameter not found in manifest for key [{key_to_check}], not proceeding.")
                continue
            for key in override_template:
                analysis_node = override_template[key]
                if override["override"] == "BYPASS":
                    self.logger.debug(f"Overriding [{key}] Status from [{analysis_node['Status']}] to ['BYPASS']")
                    analysis_node["Status (Pre-Override)"] = analysis_node["Status"]
                    analysis_node["Status"] = "BYPASS"
                else:
                    analysis_node["Golden Spec Value (Pre-Override)"] = analysis_node["Golden Spec Value"]
                    analysis_node["Golden Spec Value"] = override["override"]
                    analysis_node["Status (Pre-Override)"] = analysis_node["Status"]
                    status = analysis_node["Status"]
                    if isinstance(override["override"], list):
                        status = analysis_node["Inventoried Value"] in override["override"]
                    elif isinstance(override["override"], str):
                        status = analysis_node["Inventoried Value"] == override["override"]
                    analysis_node["Status"] = status
                    self.logger.debug(f"Overriding [{key}] Golden Spec Value to [{override['override']}] and Status to [{status}]")
                override_result[key] = analysis_node

        return override_result

    def _check_valid_analysis_result(self, analysis_result):
        """ Checks that the analysis result is in the expected structure """
        # Check that the result is populated
        if not analysis_result:
            # TODO: Should this be a failure or is it okay as a warning? Could there be a case where we don't analyze any keys?
            self.logger.warning("Analysis result is empty")
        # Check that the result is a dict
        elif not isinstance(analysis_result, dict):
            error_message = "Analysis result is not a dictionary"
            self.logger.error(error_message)
            return False, error_message
        # Check that all analysis nodes are dict
        elif not all(isinstance(analysis_result[k], dict) for k in analysis_result):
            error_message = "One or more analysis results is not a dictionary"
            self.logger.error(error_message)
            return False, error_message
        # Check that all analysis nodes have the expected structure
        elif not all(set(ExpectedKeys).issubset(analysis_result[k]) for k in analysis_result):
            error_message = f"One or more analysis results does not contain all of the required keys: {ExpectedKeys}"
            self.logger.error(error_message)
            return False, error_message
        return True, ""

    def run(self):
        """ Runs analysis """
        # Init error flag
        error = 0

        # Run all plugin analysis
        for plugin in self.plugin_interface.get_plugins():
            # Look for "analysis" method from plugin's function manifest
            # !NOTE: We make the assumption that each plugin has exactly 1 analysis method
            analysis_methods = plugin_helper.get_plugin_method(plugin, "analysis")
            if not analysis_methods:
                # Use base_analysis function from base plugin by default
                self.logger.warning(f"Using default base analysis function for plugin: {plugin}")
                method_object = {"functionName": "base_analysis"}
            else:
                method_object = analysis_methods[0]

            # Check if plugin inventory was successful
            inventory_result = utils.get_latest_helper_results("inventory").get(plugin)
            if not inventory_result:
                inventory_result = {
                    "results": {},
                    "exit_code": ANALYSIS_ERROR,
                    "exit_message": "Failed to find latest plugin inventory data",
                    "next_action_hint": "stop"
                }
                self.logger.error("Failed to find latest plugin inventory data")
            if inventory_result["exit_code"] != 0:
                self.total_errors += 1
                error += 1
                self._generate_generic_failure(plugin,
                                               failure_item="Inventory Scan",
                                               failure_message=miscellaneous.prettify_string(inventory_result["exit_message"]))
                continue

            # Init args
            args = []

            # Dump inventory data and add to function args
            inv_dump_path = self.plugin_interface.dump_plugin_inventory_data(plugin)
            if not inv_dump_path:
                self.total_errors += 1
                error += 1
                self._generate_generic_failure(plugin,
                                               failure_item="Plugin Inventory Data",
                                               failure_message="Failed to dump inventory data")
                continue
            args += ["--inventory_data_json", inv_dump_path]

            # Dump golden data and add to function args
            golden_dump_path = self.plugin_interface.dump_plugin_golden_data(plugin)
            if not golden_dump_path:
                self.total_errors += 1
                error += 1
                self._generate_generic_failure(plugin,
                                               failure_item="Golden Spec Data",
                                               failure_message="Failed to dump golden data")
                continue
            args += ["--golden_data_json", golden_dump_path]

            # Check and add additional manifest parameters
            args += self.plugin_interface.get_override_args(plugin, method_object["parameters"])

            # Run plugin method
            func_type = "analysis"
            result = self.run_method(plugin, method_object, args, func_type)
            exit_code = result["exit_code"]
            exit_message = miscellaneous.prettify_string(result["exit_message"])

            # Add to exit code
            error += exit_code

            # Delete temp dump files after analysis runs
            for file in [inv_dump_path, golden_dump_path]:
                self.logger.debug(f"Removing temporary file: {file}")
                os.remove(file)

            # Execution failure if exit code is <60000
            # >60000 exit could imply a custom failure so we don't want to always exclude those results
            ok_exit_codes = [0, 8, 92, 93] # TODO: these are all possible "ok" exits from base_analysis func
            if (exit_code in range(1, 60000) and exit_code not in ok_exit_codes):
                self.total_errors += 1
                self._generate_generic_failure(plugin,
                                               failure_item="Plugin Analysis",
                                               failure_message=exit_message)
                continue

            # Check that the analysis result is valid
            is_valid, validation_error = self._check_valid_analysis_result(result["results"])
            if not is_valid:
                if exit_message.lower() != "success":
                    failure_message = exit_message
                else:
                    failure_message = validation_error
                self.total_errors += 1
                self._generate_generic_failure(plugin,
                                               failure_item="Plugin Analysis",
                                               failure_message=failure_message)
                continue

            # Check for overrides in the manifest
            override_results = self.check_analysis_overrides(plugin, result["results"], self.analysis_type)
            result["results"].update(override_results)

            # Record result
            self.results[plugin] = result
            if "duration_in_sec" in result:
                self.logger.info("Finished in {:.2f}s\n".format(result["duration_in_sec"]))
            else:
                self.logger.info(f"Finished {func_type}\n")

            # Add to report generator table
            # Get all items of classification needed
            classified_items = list(
                find_all_dictionaries(
                    self.results[plugin]["results"], # Node
                    "Class", # Key
                    AnalysisTypes.get(self.analysis_type, DefaultType)["classifications"] # Classification type(s) (value)
                )
            )

            # Get classified failures
            classified_failures = list(
                find_all_dictionaries(
                    classified_items, # Node
                    "Status", # Key
                    [False] # Classification type(s) (value)
                )
            )

            # Get classified bypass
            classified_bypass = list(
                find_all_dictionaries(
                    classified_items, # Node
                    "Status", # Key
                    ["BYPASS"] # Classification type(s) (value)
                )
            )

            # Update counts
            self.total_analyzed += len(classified_items)
            self.total_failures += len(classified_failures)
            self.total_bypass += len(classified_bypass)

            # Loop through classified failures and gather info
            for failure_item in classified_failures:
                for failure_key in failure_item:
                    # Set defaults
                    golden_spec_value = f"Refer to {plugin} in golden spec"
                    actual_value = f"Refer to {plugin} get_inventory"

                    # Try to get golden spec value from json
                    try:
                        golden_spec_value = list(find_keys(failure_item[failure_key], "Golden Spec Value"))[0]
                    except Exception as ex:
                        self.logger.error(f"Failed to get golden spec value from {failure_key} for {plugin}.\nERROR: {ex}")
                        self.total_errors += 1

                    # Try to get actual value on the system from json
                    try:
                        actual_value = list(find_keys(failure_item[failure_key], "Inventoried Value"))[0]
                    except Exception as ex:
                        self.logger.error(f"Failed to get inventoried value from {failure_key} for {plugin}.\nERROR: {ex}")
                        self.total_errors += 1

                    # Add quotes to inventory/golden values if there is leading/trailing whitespace
                    # otherwise it will not be visible in the report gen table
                    if isinstance(actual_value, str) and (actual_value != actual_value.strip()):
                        if isinstance(golden_spec_value, str):
                            # also adding the quotes to golden_spec_value for consistency
                            golden_spec_value = f"'{golden_spec_value}'"
                        actual_value = f"'{actual_value}'"
                    if isinstance(golden_spec_value, str) and (golden_spec_value != golden_spec_value.strip()):
                        if isinstance(actual_value, str):
                            # also adding the quotes to actual_value for consistency
                            actual_value = f"'{actual_value}'" if not actual_value.startswith("'") else actual_value
                        golden_spec_value = f"'{golden_spec_value}'" if not golden_spec_value.startswith("'") else golden_spec_value

                    # Add row to table
                    self.result_table.add_row([plugin_helper.get_component_type(plugin),
                                               failure_key,
                                               str(golden_spec_value),
                                               actual_value])

        # Update result count table
        self.result_count_table.add_row([self.total_analyzed, self.total_failures, self.total_bypass, self.total_errors])

        # Call parent base virtual run() method
        super().run()

        return self.results, error
