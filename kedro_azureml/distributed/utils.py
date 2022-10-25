import json
import logging
import os

logger = logging.getLogger()


def is_distributed_master_node() -> bool:
    is_rank_0 = True
    try:
        if "TF_CONFIG" in os.environ:
            # TensorFlow
            tf_config = json.loads(os.environ["TF_CONFIG"])
            worker_type = tf_config["task"]["type"].lower()
            is_rank_0 = (worker_type == "chief" or worker_type == "master") or (
                worker_type == "worker" and tf_config["task"]["index"] == 0
            )
        else:
            # MPI + PyTorch
            for e in ("OMPI_COMM_WORLD_RANK", "RANK"):
                if e in os.environ:
                    is_rank_0 = int(os.environ[e]) == 0
                    break
    except:  # noqa
        logger.error(
            "Could not parse environment variables related to distributed computing. "
            "Set appropriate values for one of: RANK, OMPI_COMM_WORLD_RANK or TF_CONFIG",
            exc_info=True,
        )
        logger.warning("Assuming this node is not a master node, due to error.")
        return False
    return is_rank_0


def is_distributed_environment() -> bool:
    return any(e in os.environ for e in ("OMPI_COMM_WORLD_RANK", "RANK", "TF_CONFIG"))
