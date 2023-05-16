import websockets.sync.client
import json
import base64

class Controller:
  def __init__(self):
    self.ws_client = websockets.sync.client.connect("ws://localhost:9002")
  
  def raw_invoke(self, cmd: str, debug=False):
    self.ws_client.send(cmd)
    recv = self.ws_client.recv()
    res = json.loads(recv)
    if not debug:
      res = res["results"]
    else:
      for log in res["logs"]:
        log["message"] = base64.b64decode(log["message"]).decode('utf-8')
    return res

  def pair(self, code: str, debug=False):
    return self.raw_invoke(f"pairing code 1 {code} --bypass-attestation-verifier 1", debug=debug)

  def on(self):
    return self.raw_invoke('onoff on 1 1')

  def off(self):
    return self.raw_invoke('onoff off 1 1')

  def detach(self):
    self.ws_client.close()