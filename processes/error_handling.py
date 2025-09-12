"""Module for handling errors"""

import traceback
import smtplib
from email.message import EmailMessage
import base64
from io import BytesIO

from PIL import ImageGrab

from mbu_dev_shared_components.database.connection import RPAConnection


def send_error_email(error: Exception, add_screenshot: bool = False, process_name: str | None = None):
    rpa_conn = RPAConnection(db_env="PROD", commit=False)
    with rpa_conn:
        error_email = rpa_conn.get_constant("ErrorEmail")["value"]
        # error_sender = rpa_conn.get_constant("ErrorSender")["value"]  # Find in database...
        smtp_server = rpa_conn.get_constant("smtp_server")["value"]
        smtp_port = rpa_conn.get_constant("smtp_port")["value"]
    # Create message
    msg = EmailMessage()
    msg['to'] = error_email
    msg['from'] = error_sender
    msg['subject'] = (
        "Error screenshot"
        + f": {process_name}" if process_name else ""
    )

    if add_screenshot:
        screenshot = grab_screenshot()

    # Create an HTML message with the exception and screenshot
    html_message = (
        f"""
        <html>
            <body>
                <p>Error type: {type(error).__name__}</p>
                <p>Error message: {str(error)}</p>
                <p>{traceback.format_exc()}</p>
        """ +
        f'      <img src="data:image/png;base64,{screenshot}" alt="Screenshot">' if add_screenshot else '' +
        """
            </body>
        </html>
        """
    )

    msg.set_content("Please enable HTML to view this message.")
    msg.add_alternative(html_message, subtype='html')

    # Send message
    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.starttls()
        smtp.send_message(msg)


def grab_screenshot():
    """Grabs screenshot"""
    # Take screenshot and convert to base64
    screenshot = ImageGrab.grab()
    buffer = BytesIO()
    screenshot.save(buffer, format='PNG')
    screenshot_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

    return screenshot_base64
