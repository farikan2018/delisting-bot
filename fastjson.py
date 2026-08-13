"""orjson, якщо встановлений — інакше stdlib.

Price-cache парсить ~86 повідомлень/с на 2-vCPU машині. orjson ~3x швидший і створює
менше обʼєктів → менше роботи GC, менше шансів, що пауза збирача впаде саме на ордер.
"""
try:
    from orjson import loads  # noqa: F401
    NAME = "orjson"
except ImportError:  # pragma: no cover
    from json import loads  # noqa: F401
    NAME = "json"
