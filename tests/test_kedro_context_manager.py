from kedro.framework.context import KedroContext

from kedro_azureml.utils import KedroContextManager


#
# def test_can_create_context_manager():
#     with KedroContextManager("tests", "local") as mgr:
#         assert mgr is not None and isinstance(
#             mgr, KedroContextManager
#         ), "Invalid object returned"
#         assert isinstance(mgr.context, KedroContext), "No KedroContext"
