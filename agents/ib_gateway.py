# agents/ib_gateway.py
"""Shared connection settings for the IBKR Client Portal Gateway
(the gateway/ folder in this repo).

Start it with:  cd gateway && bin/run.sh root/conf.yaml   (or ./start.sh)
then log in once via a browser at https://localhost:5001
(root/conf.yaml sets listenPort: 5001 with SSL enabled).
"""
import os
import ssl

GATEWAY_BASE_URL = os.environ.get("IB_GATEWAY_URL", "https://localhost:5001/v1/api")
GATEWAY_WS_URL = "wss" + GATEWAY_BASE_URL[len("https"):] + "/ws"

TICKER = os.environ.get("TICKER", "TSLA")


def ssl_context():
    # The gateway serves a self-signed certificate (vertx.jks), so
    # certificate and hostname verification must be disabled.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx
