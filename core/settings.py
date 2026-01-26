import os
from datetime import datetime
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
BUCKET_NAME = os.environ.get("UPLOAD_BUCKET", "ank-bucket")
MAX_DIALOGUE_PER_MONTH = int(os.environ.get("MAX_DIALOGUE_PER_MONTH", "5"))

def month_key_jst() -> str:
    return datetime.now(tz=JST).strftime("%Y-%m")
