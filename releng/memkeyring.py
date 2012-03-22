from keyring.backend import KeyringBackend

class MemKeyring(KeyringBackend):
    """Memory only Keyring
    """
    def __init__(self):
        self.keyring = {}

    def supported(self):
        return 0

    def get_password(self, service, username):
        if username in self.keyring:
            return self.keyring[username]
        else:
            return None

    def set_password(self, service, username, password):
        self.keyring[username] = password
        return 0
