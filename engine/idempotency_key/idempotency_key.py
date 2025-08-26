import uuid

class IdempotencyKey():
    def __init__(self):
        self.key = self.create_key()
        self.result = None

    def __eq__(self, other: 'IdempotencyKey'):
        return self.key == other.key

    def create_key(self):
        return uuid.uuid4()

    def save_result(self, res):
        self.result = res
    
