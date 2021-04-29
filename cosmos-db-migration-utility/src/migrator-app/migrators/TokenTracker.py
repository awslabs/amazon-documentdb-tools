import copy
import threading
from datetime import datetime

from common.Singleton import Singleton
from common.logger import get_logger

logger = get_logger(__name__)


class TokenTracker(metaclass=Singleton):
    def __init__(self):
        self.__tokens = {}
        self.__event = threading.Event()

    def update_token(self, key, change):
        # waits for any active read to complete
        self.__event.wait()
        # store the token value for given key
        self.__tokens[key] = {
            "last_changed_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "token_id": change["_id"]
            # , "change": change
        }

    def get_token(self):
        try:
            # explicitly lock the token object for any updates
            self.__event.clear()
            # return the token to the client
            return {
                "system_datetime": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "resume_tokens": copy.deepcopy(self.__tokens)
            }
        finally:
            # release any lock 
            self.__event.set()
