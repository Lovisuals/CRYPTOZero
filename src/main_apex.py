import asyncio
import os

from src.main import main

if __name__ == "__main__":
    os.environ.setdefault("CRYPTZERO_APEX_ENTRYPOINT", "1")
    asyncio.run(main())
