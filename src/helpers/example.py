from helpers.database import DBConnectorFactory
from helpers.mixpanel_raw_files import load_mp_json, calc_hash
import logging

logger = logging.getLogger(__name__)

db_factory = DBConnectorFactory()

s3 = S3resources(os.getenv("AWS_S3_BUCKET"))

