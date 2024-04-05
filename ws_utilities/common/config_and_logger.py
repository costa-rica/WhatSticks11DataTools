import os
from ws_config import ConfigWorkstation, ConfigDev, ConfigProd
import logging
from logging.handlers import RotatingFileHandler

match os.environ.get('WS_CONFIG_TYPE'):
    case 'dev':
        config = ConfigDev()
        print('- WhatSticks11DataTools(ws_utilites)/config: Development')
    case 'prod':
        config = ConfigProd()
        print('- WhatSticks11DataTools(ws_utilites)/config: Production')
    case _:
        config = ConfigWorkstation()
        print('- WhatSticks11DataTools(ws_utilites)/config: Local')

#Setting up Logger
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter_terminal = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(message)s')

#initialize a logger
logger_ws_utilities = logging.getLogger(__name__)
logger_ws_utilities.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.DIR_LOGS,'ws_utilities.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will logger_ws_utilities
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_ws_utilities.addHandler(file_handler)
logger_ws_utilities.addHandler(stream_handler)

################################################################################################
# Probably should not use this. Instead, pass in db_session (from app that called ws_utilities)
# that needs operation done and return db_session to source app to commit/rollback and close
################################################################################################
def wrap_up_session(db_session):
    logger_ws_utilities.info("- accessed wrap_up_session -")
    try:
        # perform some database operations
        db_session.commit()
        logger_ws_utilities.info("- perfomed: db_session.commit() -")
    except Exception as e:
        logger_ws_utilities.info(f"{type(e).__name__}: {e}")
        db_session.rollback()  # Roll back the transaction on error
        logger_ws_utilities.info("- perfomed: db_session.rollback() -")
        raise
    finally:
        db_session.close()  # Ensure the session is closed in any case
        logger_ws_utilities.info("- perfomed: db_session.close() -")