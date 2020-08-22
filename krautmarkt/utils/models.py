from abc import abstractclassmethod
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)


class SourceModel():
    """
    Model is the base class for
    """
    def __init__(self) -> None:
        pass

    @abstractclassmethod
    def fetch_metadata(self):
        raise NotImplementedError(f"Please implement this method")


class SaveModel():
    """
    SaveModel
    """

    def __init__(self) -> None:
        pass

    @abstractclassmethod
    def save_markdown(self):
        raise NotImplementedError("Please implement this method")

    @abstractclassmethod
    def save_json(self):
        raise NotImplementedError("Please implement this method")
