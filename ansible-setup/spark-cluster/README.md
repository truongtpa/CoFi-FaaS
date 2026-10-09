# Test connection
ansible all -m ping

# Chạy playbook
ansible-playbook playbook.yml

# Chạy với verbose mode
ansible-playbook playbook.yml -v

# Chạy chỉ một phần cụ thể
ansible-playbook playbook.yml --tags "java"

# Echo SSH key
```
cat > ~/.ssh/id_rsa << 'EOF'
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAkeJTWZPin5xVE6eEUU8gyxA5wAcAb5yb3kMVFKtmk3emwjRX
N05QPkHWa9//XXTTI6B4KES20ArUbR0o07/eIHua73aLNhBzwC0Zfo2AyJloqXwH
VuRJJGoUdu8PF2m5MCzvUlylHZ6erms5uwVceto7u8eohXd3QZe2vL5MwzgpFjnG
oabIYfLgOjMJ/tzjUuEWHdYqxC0WHF/iqxalckdCGVQ4x+oz4EkUu7kGz+w0tkLC
K2t4fF0INK/MukeSZMsYpjFPyuNjtJnSygE9x5++dkpZKz6NyxLftWDDMazscwDW
mmYyNsiSbk6Xn5UKnU8DqMEZcBxNgBgjKqT/WQIDAQABAoIBACRA4OLFeA+fO5tH
SYAlUGp2TUu10btq6WdKN25sC/FAzl27wSLa4OEf3mfvghgZBLF5WvLy5JV94318
Ph2lNE/RN9cjmAPnAcTz0D6dbrArQ5G+41oKIE0e2ZgW36K7YMyilhTbNiNOvHNu
7SlXczyKiEapnu0QG8BesghkqFHo/l4wEhamqadYUU0U0C9GupYk7jFTfoZlXYFf
FwaEYTkz+hjU0AfKVznxcbOjMRgfyBtIzJmKUp8NPlIvC98lZEgO0fVUOsJZ+BgC
M4Z8Tp2ZSQxIHirjgazlrhiYTtZGubS9fCpYSmkG4ExoL335J/WQZd4QnI55Qsr3
pr0ctIsCgYEAx85eJ7bs/RedIpaTQZU1Sb3tX0TuRd3s7UuVvYID+4xzpFCo4ZZX
6h3PBu+kUDi4otUgDX4FRMLZXFEVF3TBBL0Ffj42N4ngY0iaoM4IVzy5JzmarhbO
Rcg/69yBBLYA/atg/GHjTm9LxVpgA/TXg6viMu/J3IKbIdbqap8UnGsCgYEAuumu
ZOl44S2mJ49kgQqRFdS+1O+35mDxg2XW1OZfbeIRFjrtxPJcgcKnpKuZqDIB50r+
WcGHLVA/2RK7CEs8MqtcqnndlvO0UMR80zzidfPAuPXLAO0xzJEmWu7RjyU3+jMo
6WZQ2IUQdrtBjHklYW1kPe9+ZFc07nMDbUnchEsCgYEAlbvAcbCzLwP4PQhseFRx
575OWfvVOESUnkvrfmYlx+g8bWII/W1mmssV75O9JmUrcNOYvqO3HQ2MSJN24+oa
EAv7Rt0mUj6gCqdJQcLFG2MlOhEAKwBqOn9T79SCV15xpag9+UT+TDQ5qz8L/0sI
CuPPku6B7x9tVFGzwI1Qq9sCgYAJZbxV6IMiVyg2bvgvoLdgsJyTmiXV2GPsJk+h
zp9Xp/kR9A6GS2UpRP8mwVXtZe5Akb2uB5PjTFiZcl6i8R1qssDq9UuVzlTvhqF8
AWCla4byBbGaL1LEpTuXcNBqcQWad74D1hgUesQ0JAAMrv6ZImXa01K4esX3gyLV
+NO0dwKBgBP7i+oUNi4OHiMD5xxIHEomQ1zCfI5WjgmiHbncEXV5AHFSUQmXeLsZ
UvFG4ACFArThb9Yz5Mwkq+ht/pKPIncRNSs/CFntuHeA0lrvWya9pygMqa8a6Cb6
4xIXfP4IHBC6IXBKbTXPSjGoU3+iJ4p1k4q55LlM03iGwk5wCJBu
-----END RSA PRIVATE KEY-----
EOF
chmod 600 ~/.ssh/id_rsa
```

# Hoặc

# Start cluster từ máy local
ansible-playbook manage-cluster.yml -e "cluster_action=start"

# Stop cluster
ansible-playbook manage-cluster.yml -e "cluster_action=stop"

# Restart cluster
ansible-playbook manage-cluster.yml -e "cluster_action=restart"

# Check status
ansible-playbook manage-cluster.yml -e "cluster_action=status"

# Start cluster
ansible-playbook start-spark.yml

# Stop cluster
ansible-playbook stop-spark.yml