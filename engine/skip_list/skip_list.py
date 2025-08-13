import random

class Node:
    left: 'Node | None'
    right: 'Node | None'
    bottom: 'Node | None'
    top: 'Node | None'

    def __init__(self, data, left=None, right=None, bottom=None, top=None):
        self.data = data
        self.left = left
        self.right = right
        self.bottom = bottom
        self.top = top

class SkipList():
    def __init__(self):
        self.layers = []
        self.height = 0
        self.__create_layer()

    def get_height(self):
        return self.height

    def __flip_coin(self):
        return random.randint(0, 1) == 1
    
    def __create_layer(self):
        is_first_layer = self.height == 0
        below_left = None if is_first_layer else self.layers[self.height - 1][0]
        below_right = None if is_first_layer else self.layers[self.height - 1][-1]

        neg_node = Node(float('-inf'), left=None, right=None, bottom=below_left, top=None)
        pos_node = Node(float('inf'),   left=neg_node, right=None, bottom=below_right, top=None)
        neg_node.right = pos_node

        if not is_first_layer:
            below_left.top = neg_node
            below_right.top = pos_node

        self.layers.append([neg_node, pos_node])
        self.height += 1

    def search(self, key):
        node = self.layers[self.height - 1][0]
        while True:
            while node.right and node.right.data < key:
                node = node.right
            
            if node.right and node.right.data == key:
                return node.right
            
            if node.bottom:
                node = node.bottom
            else:
                return node

    def __find_predecessors(self, key):
        # builds table of predecessors at each level
        preds = [None] * self.height
        node = self.layers[self.height - 1][0]
        level = self.height - 1
        while True:
            while node.right and node.right.data < key:
                node = node.right
            preds[level] = node
            if node.bottom:
                node = node.bottom
                level -= 1
            else:
                break
        return preds

    def insert(self, key):
        preds = self.__find_predecessors(key)
        pred = preds[0]

        # duplicate check
        if pred.right and pred.right.data == key:
            return pred.right

        # insert at base level
        right = pred.right
        new_node = Node(key, left=pred, right=right, bottom=None, top=None)
        pred.right = new_node
        if right:
            right.left = new_node

        # promote with coin flips
        level = 1
        lower = new_node
        while self.__flip_coin():
            if level >= self.height:
                # add new top layer and use its -inf as predecessor
                self.__create_layer()
                preds.append(self.layers[self.height - 1][0])

            pred = preds[level]
            right = pred.right
            upper = Node(key, left=pred, right=right, bottom=lower, top=None)
            pred.right = upper
            if right:
                right.left = upper
            lower.top = upper
            lower = upper
            level += 1

        return new_node
    
    def contains(self, key):
        node = self.layers[self.height - 1][0]
        while True:
            while node.right and node.right.data < key:
                node = node.right
            
            if node.right and node.right.data == key:
                return True
            
            if node.bottom:
                node = node.bottom
            else:
                return False


    def delete(self, key):
        preds = self.__find_predecessors(key)
        found = False

        for level in range(self.height - 1, -1, -1):
            pred = preds[level]
            curr = pred.right
            if curr and curr.data == key:
                found = True
                forward = curr.right
                pred.right = forward
                if forward:
                    forward.left = pred
                if curr.bottom:
                    curr.bottom.top = None
                curr.left = curr.right = curr.top = curr.bottom = None

        if not found:
            return

        while self.height > 1 and self.layers[-1][0].right is self.layers[-1][-1]:
            neg, pos = self.layers.pop()
            self.layers[-1][0].top = None
            self.layers[-1][-1].top = None
            self.height -= 1

            



