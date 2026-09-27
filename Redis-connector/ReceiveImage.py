import redis        # pip install redis
import base64
import os
from dotenv import load_dotenv   # pip install python-dotenv

load_dotenv()

ip       = os.getenv('REDIS_HOST')
port     = int(os.getenv('REDIS_PORT', 6379))
password = os.getenv('REDIS_PASSWORD')

r = redis.Redis(host=ip, port=port, db=0, password=password)

value=r.get('image')
decoded_value=base64.b64decode(value)

with open("./received.jpg", "wb") as f:
    f.write(decoded_value)
    
print('Image received, check ./received.jpg')
