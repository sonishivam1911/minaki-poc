import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, TypeVar, Optional

# Type variable for function return type
T = TypeVar('T')

def make_async(
    max_concurrency: Optional[int] = None,
    thread_name_prefix: str = "async_worker"
) -> Callable[[Callable[..., T]], Callable[..., asyncio.Future[T]]]:
    """
    Decorator that transforms any function into an asynchronous function.
    
    If the function is already a coroutine function, it returns it unchanged.
    If the function is synchronous, it wraps it to run in a thread pool.
    
    Args:
        max_concurrency: Maximum number of threads in the pool. Default is None (uses Python's default).
        thread_name_prefix: Prefix for thread names in the pool.
    
    Returns:
        A decorator function that converts the decorated function to an async function.
    
    Usage:
        @make_async()
        def my_function(x, y):
            # This will run in a thread pool
            return x + y
        
        # Now you can await it
        result = await my_function(1, 2)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., asyncio.Future[T]]:
        # If the function is already a coroutine function, return it as is
        if asyncio.iscoroutinefunction(func):
            return func
        
        # Create a thread pool executor for this function
        executor = ThreadPoolExecutor(
            max_workers=max_concurrency,
            thread_name_prefix=thread_name_prefix
        )
        
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            # Use get_running_loop() instead of new_event_loop() to work with existing event loops
            loop = asyncio.get_running_loop()
            
            # Run the synchronous function in the thread pool
            return await loop.run_in_executor(
                executor,
                lambda: func(*args, **kwargs)
            )
        
        return wrapper
    
    return decorator