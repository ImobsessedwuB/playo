"""Entrypoint untuk Railway deployment."""
import asyncio
import sys
import os

# Pastikan root project ada di sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.main import main

if __name__ == "__main__":
    asyncio.run(main())
