from dotenv import load_dotenv


class Config:
    def __init__(self):
        self.conf = dict()
        self.init_from_envs()

    def init_from_envs(self):
        load_dotenv()
