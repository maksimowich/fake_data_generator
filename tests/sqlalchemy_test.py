from datetime import datetime
import pytz
d = datetime.now(tz=pytz.timezone('Europe/Moscow')).replace(microsecond=0)
print(d.tzinfo)