"""
This is the main entry point for the process
"""

import asyncio
import logging
import sys

from automation_server_client import AutomationServer, Workqueue
from mbu_rpa_core.exceptions import BusinessError, ProcessError
from mbu_rpa_core.process_states import CompletedState

from processes.item_retriever import item_retriever
from processes.process_item import process_item
from processes.finalize_process import finalize_process
from processes.error_handling import handle_error
from helpers import ats_functions


async def populate_queue(workqueue: Workqueue):
    """Populate the workqueue with items to be processed."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from populate workqueue!")

    items_to_queue = item_retriever()  # Replace with actual source of items

    queue_references = ats_functions.get_workqueue_items(workqueue)

    for item in items_to_queue:
        reference = item.get("reference")  # Unique identifier for the item

        data = {"item": item}

        # Add item if reference is not already in queue
        if reference not in queue_references:
            work_item = workqueue.add_item(data, reference)
            logger.info(f"Added item to queue: {work_item}")

        else:
            logger.info(f"Reference: {reference} already in queue. Item: {item} not added")
            print(f"Reference: {reference} already in queue. Item: {item} not added")


async def process_workqueue(workqueue: Workqueue):
    """Process items from the workqueue."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data, reference = ats_functions.get_item_info(item)  # Item data deserialized from json as dict

            try:
                process_item(data, reference)

                completed_state = CompletedState.completed("Process completed without exceptions")  # Adjust message for specific purpose
                item.complete(str(completed_state))

                continue

            except ProcessError as e:
                # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
                handle_error(error=e, log=logger.error, action=item.fail, item=item, send_mail=True, process_name=workqueue.name)

                raise ProcessError("Process Failed") from e

            except BusinessError as e:
                # A BusinessError indicates a breach of business logic or something else to be handled by business department
                handle_error(error=e, log=logger.info, item=item, action=item.pending_user)


async def finalize(workqueue: Workqueue):
    """Finalize process."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from finalize!")

    try:
        finalize_process()

    except ProcessError as e:
        # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
        handle_error(error=e, log=logger.error, send_mail=True, process_name=workqueue.name)

        raise ProcessError from e

    except BusinessError as e:
        # A BusinessError indicates a breach of business logic or something else to be handled by business department
        handle_error(error=e, log=logger.info)


if __name__ == "__main__":

    logger = logging.getLogger(__name__)

    logger.info("Starting")

    ats = AutomationServer.from_environment()

    prod_workqueue = ats.workqueue()
    process = ats.process

    print(f"{ats = }, {prod_workqueue = }, {process = }")

    logger.info(f"{process = }")

    # Queue management
    if "--queue" in sys.argv:
        asyncio.run(populate_queue(prod_workqueue))

    if "--process" in sys.argv:
        # Process workqueue
        asyncio.run(process_workqueue(prod_workqueue))

    if "--finalize" in sys.argv:
        # Finalize process
        asyncio.run(finalize(prod_workqueue))

    sys.exit(0)
