from typing import Dict, Any, Union, Callable, Deque, Tuple
from collections import deque
from .Publisher import Publisher 
from .Consumer import Consumer
from functools import wraps
import asyncio

class EventRouter():
    def __init__(self):
        self.events: Dict[str, Any] = {}
        self.ordered_event_names: Deque = deque()

    def publish(self, publisher: Union[Publisher, Tuple(str, Callable)]):
        if not isinstance(publisher, Publisher):
            publisher = Publisher(name=publisher[0], action=publisher[1])

        publisher_name = publisher.name
        if not publisher_name in self.events:
            new_pub_q = deque()
            self.events[publisher_name] = new_pub_q
        
        self.events[publisher_name].append(publisher.action)
        self.ordered_event_names.append(publisher_name)

    async def run_event(self, name: str, func: Callable, ):
        if name not in self.events:
            return 

    async def run_events(self):
        while True:
            if len(self.ordered_event_names) == 0:
                continue

            curr_event_name = self.ordered_event_names.popleft()
            curr_event_action = self.events[curr_event_name].popleft()

            async def run_action():
                task = asyncio.create_task(Consumer.run_consumer(curr_event_action))
                await task

            asyncio.run(run_action())



        

        



