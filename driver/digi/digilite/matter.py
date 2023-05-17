import websockets.sync.client
import json
import base64

class Cluster:
  def __init__(self, controller, cluster: int | str):
    self.controller = controller
    self.cluster = cluster

class UndefinedCluster(Cluster):

  def raw_invoke(self, cmd: str, payload, endpoints: list[str | int] | str | int, debug: bool):
    endpoints = endpoints if type(endpoints) == list else [endpoints]
    endpoints = map(lambda e: str(e), endpoints)
    endpoints = ','.join(endpoints)
    return self.controller.raw_invoke(f"any {cmd.replace('_', '-')} {self.cluster} {payload} {self.controller.node_id} {endpoints}", debug=debug)

  def __getattr__(self, cmd):
      def dynamic_method(payload: str = "", endpoints: list[str | int] | str | int = "0xFFFF", debug=False):
        nonlocal cmd
        if type(cmd) != str:
          payload = f"{cmd} payload"
          cmd = "command_by_id"
        
        aliases = {
          "read": "read-by-id",
          "read_event": "read-event-by-id",
          "write": "write-by-id"
        }

        if cmd in aliases:
          cmd = aliases[cmd]

        return self.raw_invoke(cmd, payload=payload, endpoints=endpoints, debug=debug)
      return dynamic_method

class DefinedCluster(Cluster):
  def raw_invoke(self, cmd: str, payload, endpoints: list[str | int] | str | int, debug: bool):
    endpoints = endpoints if type(endpoints) == list else [endpoints]
    endpoints = map(lambda e: str(e), endpoints)
    endpoints = ','.join(endpoints)
    return self.controller.raw_invoke(f"{self.cluster} {cmd.replace('_', '-')} {payload} {self.controller.node_id} {endpoints}", debug=debug)

  def __getattr__(self, cmd: str):
      def dynamic_method(payload: str = "", endpoints: list[str | int] | str | int = "0xFFFF", debug=False):
        return self.raw_invoke(cmd, payload=payload, endpoints=endpoints, debug=debug)
      return dynamic_method

class Controller:
  def __init__(self):
    self.ws_client = websockets.sync.client.connect("ws://localhost:9002")
    self.node_id = 1
  
  def raw_invoke(self, cmd: str, debug=False) -> object:
    self.ws_client.send(cmd)
    recv = self.ws_client.recv()
    res = json.loads(recv)
    for log in res["logs"]:
      log["message"] = base64.b64decode(log["message"]).decode('utf-8')
    
    if len(res["results"]) > 0 and "error" in res["results"][0]:
      raise Exception(res["logs"])

    if not debug:
      res = res["results"]
    return res

  def pair(self, code: str, debug=False):
    return self.raw_invoke(f"pairing code {self.node_id} {code} --bypass-attestation-verifier 1", debug=debug)

  def cluster(self, cluster: int | str):
    if type(cluster) == str:
      return DefinedCluster(self, cluster)
    else:
      return UndefinedCluster(self, cluster)

  def detach(self):
    self.ws_client.close()