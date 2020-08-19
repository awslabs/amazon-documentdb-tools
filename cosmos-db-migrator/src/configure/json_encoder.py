

from datetime import datetime
from decimal import Decimal
import json

class JSONFriendlyEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super(JSONFriendlyEncoder, self).default(o)