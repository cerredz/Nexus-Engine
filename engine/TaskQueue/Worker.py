from collections import deque
from .TaskQueue import TaskQueue, TaskResult
import datetime
import asyncio

class Worker():
    def __init__(self, task_queue: 'TaskQueue', max_results: int, max_retries: int, timeout: int = 30):
        assert task_queue is not None, "task_queue cannot be None"
        assert max_results > 0 and max_retries > 0 and timeout > 0, "max_results and max_retries and timeout must be greater than 0"
        self.task_queue = task_queue
        self.max_results = max_results
        self.max_retries = max_retries
        self.results = deque([])
        self.timeout = timeout
        self.is_running = False
    
    def get_last_result(self):
        if self.results: return self.results[0]

    def get_results(self):
        return list(self.results)

    def stop(self):
        self.is_running = False 
    
    async def execute_tasks(self):
        self.is_running = True

        while self.is_running:
            curr_task = None
            args = ()
            kwargs = {}
            
            try:
                task_data = self.task_queue.dequeue()
                if task_data == None: 
                    await asyncio.sleep(0.01) 
                    continue
            
                curr_task, args, kwargs = task_data
                res = await asyncio.wait_for(asyncio.to_thread(curr_task, *args, **kwargs), timeout=self.timeout)
                if len(self.results) >= self.max_results: self.results.popleft()
                self.results.append(TaskResult(completed_at=datetime.datetime.now(), result=res))
            
            except asyncio.TimeoutError:
                print(f"Task timed out after {self.timeout} seconds. If you want to increase timeout pass timeout prop into the constructor")
                if curr_task is not None:
                    await self.__handle_task_failure(curr_task, args, kwargs)
            except Exception as e:
                if curr_task is not None:
                    await self.__handle_task_failure(curr_task, args, kwargs)
                
    async def __handle_task_failure(self, task, args, kwargs):
        if task is None:
            return

        retries = 0
        while retries < self.max_retries:
            backoff = 1 * (2 ** retries)
            await asyncio.sleep(backoff)
            try:
                res = await asyncio.wait_for(asyncio.to_thread(task, *args, **kwargs), timeout=self.timeout)
                if len(self.results) >= self.max_results: self.results.popleft()
                self.results.append(TaskResult(completed_at=datetime.datetime.now(), result=res))
                break
            except Exception as e:
                retries += 1

            
                



