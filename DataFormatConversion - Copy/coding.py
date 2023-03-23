import hmac
import hashlib

message = "sihang"
secret_key = b"my_secret_key"

hash_object = hmac.new(secret_key, message.encode(), hashlib.sha256)
hex_dig = hash_object.hexdigest()
print(hex_dig)




