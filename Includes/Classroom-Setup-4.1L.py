# Databricks notebook source
!pip install openai -qq

# COMMAND ----------

# MAGIC %pip install databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./Vocareum-Workspace-Setup"

# COMMAND ----------

import requests

class TileManager:
    def __init__(self, host: str, headers: dict):
        self.host = host.rstrip('/')
        self.headers = headers

    def get_tile(self, tile_id: str):
        url = f"{self.host}/api/2.0/tiles/{tile_id}"
        payload = {"tile_id": tile_id}
        resp = requests.get(url, headers=self.headers, json=payload)
        resp.raise_for_status()
        return resp.json()

    def get_endpoint_name(self, tile_id: str):
        tile = self.get_tile(tile_id)
        return tile.get('serving_endpoint_name')

    def list_endpoints(self):
        url = f"{self.host}/api/2.0/serving-endpoints"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def list_tiles(self, next_page_token=None):
        url = f"{self.host}/api/2.0/tiles"
        params = {"page_token": next_page_token} if next_page_token else {}
        resp = requests.get(url, headers=self.headers, params=params)
        resp.raise_for_status()
        return resp.json()

    def list_all_tiles(self):
        res = []
        next_page_token = None
        while True:
            tiles = self.list_tiles(next_page_token)
            res.extend(tiles.get("tiles", []))
            next_page_token = tiles.get("next_page_token")
            if not next_page_token:
                break
        return res

    def delete_tile_endpoint(self, endpoint_name: str):
        try:
            url = f"http://127.0.0.1:7073/api/2.0/tile-endpoints/{endpoint_name}"
            resp = requests.delete(url, headers=self.headers)
            resp.raise_for_status()
            print(f"Deleted: {endpoint_name}")
        except Exception as e:
            print(f"Error deleting tile endpoint '{endpoint_name}': {e}")

    def delete_tile_endpoint_from_tile_id(self, tile_id: str):
        try:
            endpoint_name = self.get_endpoint_name(tile_id)
            self.delete_tile_endpoint(endpoint_name)
        except Exception as e:
            print(f"Error deleting tile endpoint from tile ID '{tile_id}': {e}")

    def delete_tile(self, tile_id: str):
        try:
            url = f"{self.host}/api/2.0/tiles/{tile_id}"
            payload = {"tile_id": tile_id}
            resp = requests.delete(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            if resp.json() == {}:
                print("Successfully deleted tile")
        except Exception as e:
            print(f"Error deleting tile '{tile_id}': {e}")

    def get_tile_id_by_name(self, endpoint_name: str):
        """Return the tile_id for a given tile name (serving endpoint name)."""
        try:
            tiles = self.list_all_tiles()
            tile_id = next((tile['tile_id'] for tile in tiles if tile.get('name') == endpoint_name), None)

            if tile_id:
                return tile_id
            else:
                print(f"Tile with name '{endpoint_name}' not found.")
                return None
        except Exception as e:
            print(f"Error getting tile ID for endpoint '{endpoint_name}': {e}")
            return None
    def full_delete_with_tile_id(self, tile_id: str):
        try:
            self.delete_tile_endpoint_from_tile_id(tile_id)
            self.delete_tile(tile_id)
        except Exception as e:
            print(f"Error deleting tile '{tile_id}': {e}")
    
    def full_delete_with_endpoint_name(self, endpoint_name: str):
        try:
            tile_id = self.get_tile_id_by_name(endpoint_name)
            self.delete_tile(tile_id)
        except Exception as e:
            print(f"Error deleting tile endpoint '{endpoint_name}': {e}")

# COMMAND ----------

def run_agent_bricks_cleanup():
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

    headers = {
    "Authorization": f"Bearer {token}"
    }

    tm = TileManager(host, headers)
    list_all_tiles = tm.list_all_tiles()
    bricks_to_delete = []
    for tile_num in range(len(list_all_tiles)):
        creator_name = list_all_tiles[tile_num]['creator']
        tile_id = list_all_tiles[tile_num]['tile_id']
        # if creator_name.startswith("labuser"):
        print(creator_name)
        print(tile_id)
        bricks_to_delete.append(tile_id)
    if bricks_to_delete == []:
        print("No Bricks detected.")
    for tile_id in bricks_to_delete:
        try: 
            print(f"Deleting tile_id {tile_id}...")
            tm.full_delete_with_tile_id(tile_id) 
            print(f"✅ Deleted tile_id {tile_id}")
        except Exception as e:
            print(f"❌ Error deleting tile '{tile_id}': {e}")
