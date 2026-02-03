import os 


class EnvReader:

	_envs = {}
	_loaded = False 
	
	@classmethod
	def _load(self):

		if not self._loaded: 
			ENV_PATH = os.path.abspath(
				os.path.join(os.path.dirname(__file__), "..", "..", ".env")
			)
			with open(ENV_PATH, "r") as file:
				for line in file:
					line = line.strip()
					if not line or line.startswith("#"):
						continue
					if "=" not in line:
						continue
					key, value = line.split("=", 1)
					key = key.strip()
					value = value.strip()
					if (
						(value.startswith('"') and value.endswith('"')) or
						(value.startswith("'") and value.endswith("'"))
					):
						value = value[1:-1]
					self._envs[key] = value

			self._loaded = True 
			
	@classmethod
	def get(self, key, default=None):
		self._load()
		return self._envs.get(key, default)