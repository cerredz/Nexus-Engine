class InvertedIndex():
    def __init__(self):
        self.STOPWORDS = ["the", "is", "on", "and", "or", "but", "in", "at", "to", "for", "of", "with", "by", "from", "up", "about", "into", "through", "during", "before", "after", "above", "below", "between", "among", "around", "a", "an"]
        self.mapping: dict = {}

    def __tokenizer(self):
        pass    

    def insert(self, text):
        pass

    def rank(self, query):
        pass

    def search(self, query):
        pass

