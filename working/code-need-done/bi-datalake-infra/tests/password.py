import bcrypt

password = b"Welcome1"
hashed = bcrypt.hashpw(password, bcrypt.gensalt(rounds=10))

print("admin:" + hashed.decode())