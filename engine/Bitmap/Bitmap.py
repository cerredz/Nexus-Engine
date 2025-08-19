
class Bitmap():
    def __init__(self, b: int = 16, data: list[int] = None):
        if b < 0:
            raise ValueError("Error, cannot initialize bitmap with less than 0 bits")
        
        self.map = [0] * (2 ** b)
        self.size = 0
        self.b = b

        if data:
            self.add(data)

    def __len__(self):
        return self.size
    
    def get_bits(self):
        return self.b
    
    def get_map(self):
        return self.map

    def add(self, data: list):
        for d in data:
            if not isinstance(d, (str, int, bool, float)):
                raise ValueError("Error, must instead type str, int, bool, or float into Bitmap")
            
            if not isinstance(d, int):
                d = int(d)

            if d < 0 or d >= len(self.map):
                raise ValueError("Error, index out of range for Bitmap capacity")

            if self.map[d] == 0:
                self.size += 1
            self.map[d] = 1

    def clear(self):
        self.map = [0] * (2 ** self.b)
        self.size = 0
        
    def delete(self, data):
        for d in data:
            if not isinstance(d, int):
                d = int(d)
            if d < 0 or d >= len(self.map):
                raise ValueError("Error, index out of range for Bitmap capacity")
            self.map[d] = 0

    def contains(self, data: list):
        for d in data:
            if not isinstance(d, int):
                d = int(d)
            if d < 0 or d >= len(self.map) or self.map[d] == 0:
                return False
        
        return True
    
    @staticmethod
    def union(bitmap1: 'Bitmap', bitmap2: 'Bitmap'):
        if bitmap1.get_bits() != bitmap2.get_bits():
            raise Exception("Error creating union for bitmaps, bitmaps must have same length for set operations.")

        bits = bitmap1.get_bits()
        new_bitmap = [0] * (2 ** bits)
        for i in range(0, 2 ** bits):
            new_bitmap[i] = bitmap1.map[i] or bitmap2.map[i]

        return Bitmap(bits, new_bitmap)

    @staticmethod
    def intersect(bitmap1, bitmap2):
        if bitmap1.get_bits() != bitmap2.get_bits():
            raise Exception("Error creating intersect for bitmaps, bitmaps must have same length for set operations.")

        bits = bitmap1.get_bits()
        new_bitmap = [0] * (2 ** bits)
        for i in range(0, 2 ** bits):
            new_bitmap[i] = bitmap1.map[i] and bitmap2.map[i]

        return Bitmap(bits, new_bitmap)

    @staticmethod
    def xor(bitmap1, bitmap2):
        if bitmap1.get_bits() != bitmap2.get_bits():
            raise Exception("Error creating xor for bitmaps, bitmaps must have same length for set operations.")

        bits = bitmap1.get_bits()
        new_bitmap = [0] * (2 ** bits)
        for i in range(0, 2 ** bits):
            new_bitmap[i] = bitmap1.map[i] ^ bitmap2.map[i]

        return Bitmap(bits, new_bitmap)
    
