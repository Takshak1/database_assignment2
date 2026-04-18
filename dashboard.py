"""Compatibility module for dashboard imports.

This module re-exports the FastAPI dashboard app and helpers from
dashboard_web.py so tests and scripts can use `import dashboard`.
"""

import dashboard_web as _dashboard_web


for _name in dir(_dashboard_web):
	if _name.startswith("__"):
		continue
	globals()[_name] = getattr(_dashboard_web, _name)

