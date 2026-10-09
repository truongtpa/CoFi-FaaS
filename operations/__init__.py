from flask import Flask
import logging, sentry_sdk, sys
import os

app = Flask(__name__)

app.config['SECRET_KEY'] = 'random'
debug = bool(os.environ.get('DEBUG', False))
if debug:
    sentry_sdk.init(
        dsn="https://e9fe96774fc438656276f273605358d7@o4510731223171072.ingest.us.sentry.io/4510731225661440",
        send_default_pii=True,
    )

from operations.funcs import *
from operations.libs.CostProbe import install
install(app)
