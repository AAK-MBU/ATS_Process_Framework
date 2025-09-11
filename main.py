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
from processes.error_handling import send_error_email


async def populate_queue(workqueue: Workqueue):
    """Populate the workqueue with items to be processed."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from populate workqueue!")

    items_to_queue = item_retriever()  # Replace with actual source of items

    for item in items_to_queue:
        reference = item.get("id")  # Unique identifier for the item

        data = {"item": item}

        work_item = workqueue.add_item(data, reference)

        logger.info(f"Added item to queue: {work_item}")


async def process_workqueue(workqueue: Workqueue):
    """Process items from the workqueue."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict

            try:
                process_item(data)

                completed_state = CompletedState.completed("Process completed without exceptions")  # Adjust message for specific purpose
                item.complete(str(completed_state))

                continue

            except ProcessError as e:
                # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
                logger.error(f"Error processing item: {data}. Error: {e}")

                item.fail(str(e))

                raise ProcessError from e

            except BusinessError as e:
                # A BusinessError indicates a breach of business logic or something else to be handled by business department
                logger.info(f"A BusinessError was raised for item: {data}. Error: {e}")

                item.pending_user(str(e))


async def finalize():
    """Finalize process."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from finalize!")

    try:
        finalize_process()

    except ProcessError as e:
        # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
        logger.error(f"Error when finalizing. Error: {e}")

        raise ProcessError from e

    except BusinessError as e:
        # A BusinessError indicates a breach of business logic or something else to be handled by business department
        logger.info(f"A BusinessError was raised during finalizing. Error: {e}")


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    prod_workqueue = ats.workqueue()
    process = ats.process

    # Initialize external systems for automation here..
    try:
        # Queue management
        if "--queue" in sys.argv:
            asyncio.run(populate_queue(prod_workqueue))

            sys.exit(0)

        if "--process" in sys.argv:
            # Process workqueue
            asyncio.run(process_workqueue(prod_workqueue))

        if "--finalize" in sys.argv:
            # Finalize process
            asyncio.run(finalize())

    except ProcessError as e:
        send_error_email(e, process_name=process.name)
