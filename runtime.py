"""Тюнінг рантайму під гарячий шлях: uvloop, GC, монітор лагу event-loop.

Мотивація: сам ордер летить 165мс (фізика до матчера Bybit). Тому все, що ми ще можемо
зіпсувати — це ДОДАТИ затримку на своїй машині: пауза GC, затирання event-loop
price-cache-ом, зайві перемикання потоків. Тут — засоби це прибрати й ЗМІРЯТИ.
"""
import asyncio
import gc
import time

import logbook as log


def install_loop() -> str:
    """uvloop замість стандартного лупу: менше overhead на сокети й таймери,
    відчутно менший джитер під WS-навантаженням price-cache."""
    try:
        import uvloop
        uvloop.install()
        return "uvloop"
    except Exception:  # noqa: BLE001
        return "asyncio"


def tune_gc() -> dict:
    """Найбільший скритий ризик латентності: пауза gen2-GC (десятки-сотні мс) саме в
    момент ордера. Price-cache постійно чурнить дикти по 800+ символів, тому покоління
    заповнюються швидко. freeze() виносить стартовий heap (ccxt markets — 3200 ринків!)
    із перегляду, а високі пороги роблять gen1/gen2 рідкісними."""
    gc.collect()
    gc.freeze()  # стартовий heap більше не сканується
    old = gc.get_threshold()
    gc.set_threshold(50_000, 500, 1000)
    return {"frozen_objects": gc.get_freeze_count(), "threshold_old": old,
            "threshold_new": gc.get_threshold()}


async def loop_lag_monitor(period: float = 60.0, warn_ms: float = 25.0) -> None:
    """Міряє, наскільки event-loop «затирають»: спимо 50мс і дивимось фактичний дрейф.
    Якщо лаг великий — гарячий ордер стоятиме в черзі за price-cache, і жодні
    мережеві оптимізації не помітні. Раз на period пишемо p50/max у лог."""
    step = 0.05
    while True:
        samples = []
        deadline = time.perf_counter() + period
        while time.perf_counter() < deadline:
            t0 = time.perf_counter()
            await asyncio.sleep(step)
            samples.append((time.perf_counter() - t0 - step) * 1000)
        samples.sort()
        p50 = samples[len(samples) // 2]
        p99 = samples[int(len(samples) * 0.99) - 1]
        mx = samples[-1]
        rec = {"lag_p50_ms": round(p50, 1), "lag_p99_ms": round(p99, 1),
               "lag_max_ms": round(mx, 1), "samples": len(samples)}
        if mx >= warn_ms:
            log.event("loop_lag_high", **rec)
        else:
            log.event("loop_lag", **rec)
