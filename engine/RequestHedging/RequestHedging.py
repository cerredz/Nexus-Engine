import requests
import aiohttp
import asyncio
import datetime

class RequestHedging():
    MICROSECONDS_IN_SECOND = 1e6

    def __init__(self, url: str, delay: int, timeout: int = 30): 
        if delay <= 0:
            raise ValueError("Error, delay must be greater than 0")
        
        self.url = url
        self.delay = delay # unit is microseconds
        self.timeout = timeout # unit is seconds

    def request(self, method: str, headers: dict, body: dict):
        return asyncio.run(self.request_async(method, headers=headers, body=body))

    async def fetch_data(self, method: str, headers: dict, body: dict):
        method = method.upper()

        headers = headers or {}
        body = body or {}
        delay_seconds = self.delay / self.MICROSECONDS_IN_SECOND

        if method not in ("GET", "POST"):
            raise RuntimeError("Error, current valid methods for request hedging are either GET or POST")
        
        loop = asyncio.get_running_loop()
        start = loop.time()
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        async with aiohttp.ClientSession(timeout=timeout) as session:
             async def do_request():
                if method == "GET":
                    async with session.get(self.url, headers=headers) as response:
                        response.raise_for_status()
                        return await response.json()
                else:  # POST
                    async with session.post(self.url, json=body, headers=headers) as response:
                        response.raise_for_status()
                        return await response.json()
                    
        t1 = asyncio.create_task(do_request())
        done, _ = await asyncio.wait({t1}, timeout=delay_seconds)
        if t1 in done:
            return await t1
        
        t2 = asyncio.create_task(do_request())
        remaining = max(0.0, self.timeout - (loop.time() - start))
        if remaining == 0:
            t1.cancel(); t2.cancel()
            raise asyncio.TimeoutError("Hedged request timed out")
        
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED, timeout=remaining)
        if not done:
            for t in pending: t.cancel()
            raise asyncio.TimeoutError("Hedged request timed out")

        winner = done.pop()
        try:
            result = await winner
        except Exception:
                # If the winner failed, try the other one if still pending
            if pending:
                other = pending.pop()
                try:
                    return await other
                finally:
                    other.cancel()
            raise
        else:
            for t in pending:
                t.cancel()
            return result


