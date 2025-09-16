"""Module for handling application startup, and close"""


def startup():
    """Function for starting applications"""
    return


def soft_close():
    """Function for closing applications softly"""
    return


def hard_close():
    """Function for closing applications hard"""
    return


def close():
    """Function for closing applications softly or hardly if necessary"""
    try:
        soft_close()
    except Exception:
        hard_close()


def reset():
    """Function for resetting application"""
    close()
    startup()
