import asyncio
import logging
import sys

from automation_server_client import AutomationServer, Workqueue, WorkItemError


async def populate_queue(workqueue: Workqueue):
    """Populate the workqueue with items to be processed."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from populate workqueue!")


async def process_workqueue(workqueue: Workqueue):
    """Process items from the workqueue."""

    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict

            try:
                # Process the item here
                pass
            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    prod_workqueue = ats.workqueue()

    # Initialize external systems for automation here..

    # Queue management
    if "--queue" in sys.argv:
        prod_workqueue.clear_workqueue("new")
        asyncio.run(populate_queue(prod_workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(prod_workqueue))
