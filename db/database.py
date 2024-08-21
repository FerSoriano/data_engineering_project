from db import DataManipulation

class Database(DataManipulation):
    def __init__(self, config: dict) -> None:
        super().__init__(config)