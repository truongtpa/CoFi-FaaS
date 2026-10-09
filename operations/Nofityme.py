import requests


def sendMessage(text):
    url = f"https://api.telegram.org/bot8624263213:AAGCd521mgbcjS60vJCQoLy94fHeT1Dpmbc/sendMessage"
    requests.post(url, json={"chat_id": "6670592858", "text": text})