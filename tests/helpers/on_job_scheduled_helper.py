"""Main purpose of this file is to help test the --on-job-scheduled argument in
`kedro azureml run`. The reason it is needed is to avoid having to patch the
importlib.import_module method to return a dummy module, which for some reason
causes the rest of the run to fail.
"""


def existing_function(job):
    """Purpose of this function is to be mocked. It must still exist so that
    getattr does not return an error after importing the module with importlib"""
    return


# Purpose of this variable is to test what happens when we pass a non callable attribute
existing_attr = True
