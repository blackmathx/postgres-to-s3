from datetime import datetime, timezone
import uuid

def gen_file_id():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    rand = uuid.uuid4().hex[:8]
    return f"{ts}_{rand}"
