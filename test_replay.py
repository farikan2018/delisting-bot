"""Реплей УСІХ реальних анонсів Binance через ланцюжок детекту.

Шлях «стаття -> категорія -> тикери -> рішення торгувати» жодного разу не
спрацьовував на живому делістингу з моменту, як зʼявився fastcms. Цей тест
проганяє через нього весь архів (423 анонси за 2022-2026), тобто перевіряє
саме те, що в бою станеться рівно один раз і без права на помилку.

Порівнюємо з тим, що бачив бектест: якщо детект витягне інші тикери, ніж ті,
на яких рахувався PnL, — уся економіка стратегії про інші угоди.

Запуск: python test_replay.py [шлях_до_bt_events.json]
"""

# Тести НІКОЛИ не пишуть у бойовий events.jsonl: 2026-10-03 прогін на сервері
# залишив там 15 синтетичних подій ws_frame, тобто отруїв саме той файл, за
# яким ми судимо, чи фід віддав хоч один справжній кадр. Має стояти ДО будь-якого
# імпорту модулів бота, бо logbook читає цю змінну на імпорті.
import os
os.environ.setdefault("BOT_LOG_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs-test"))
import json
import sys
from collections import Counter
from pathlib import Path

import binance_watcher as bw

SRC = Path(sys.argv[1] if len(sys.argv) > 1 else "bt_events.json")
if not SRC.exists():
    print(f"нема {SRC} — покласти поруч bt_events.json зі скретчпада")
    sys.exit(2)

events = json.loads(SRC.read_text(encoding="utf-8"))
print(f"=== реплей {len(events)} реальних анонсів ===\n")

FAILS = []


def check(name, cond, extra=""):
    print(f"  {'OK  ' if cond else 'FAIL'}  {name}{(' — ' + extra) if extra else ''}")
    if not cond:
        FAILS.append(name)


# ---------- 1) категоризація ----------
# ВАЖЛИВО про базу порівняння: bt_events.json згенерований ДО того, як зʼявилась
# категорія MARGIN_DELIST — там margin-анонси лежать як SPOT_DELIST із окремим
# прапорцем margin_only. Тому «розбіжність» SPOT_DELIST(+margin_only) -> MARGIN_DELIST
# є ОЧІКУВАНОЮ і означає, що класифікатор новіший за базу, а не зламаний.
def expected_cat(e):
    if e.get("margin_only") and e["cat"] == "SPOT_DELIST":
        return "MARGIN_DELIST"
    return e["cat"]


cat_mismatch, tick_mismatch, upgraded = [], [], []
cats = Counter()
for e in events:
    title = e["title"]
    cat = bw.classify(title)
    cats[cat] += 1
    if cat != expected_cat(e):
        cat_mismatch.append((title[:70], expected_cat(e), cat))
    elif cat != e["cat"]:
        upgraded.append(title[:60])
    if cat == "SPOT_DELIST":
        got = sorted(bw.extract_tickers(title))
        want = sorted(e.get("tickers") or [])
        if got != want:
            # додані тикери — це виправлення старих пропусків (D, FOR були в
            # стоп-листі), втрачені — справжня регресія. Розрізняємо.
            (tick_mismatch if set(want) - set(got) else upgraded).append(
                (title[:70], want, got) if set(want) - set(got) else
                f"{title[:50]}: +{sorted(set(got)-set(want))}")

print("=== 1) КАТЕГОРИЗАЦІЯ ===")
for c, n in cats.most_common():
    print(f"    {c:<16} {n:>4}")
check("категоризація без справжніх розбіжностей", not cat_mismatch,
      f"розбіжностей {len(cat_mismatch)}")
print(f"    (переклясифіковано в MARGIN_DELIST порівняно зі старою базою: "
      f"{sum(1 for e in events if e.get('margin_only') and e['cat']=='SPOT_DELIST')})")
for t, a, b in cat_mismatch[:5]:
    print(f"      «{t}» бектест={a} детект={b}")

print("\n=== 2) ВИТЯГУВАННЯ ТИКЕРІВ (лише SPOT_DELIST) ===")
check("жоден тикер НЕ ВТРАЧЕНО проти бази", not tick_mismatch,
      f"втрачено у {len(tick_mismatch)} анонсах")
gained = [u for u in upgraded if isinstance(u, str) and ": +" in u]
print(f"    додано тикерів, яких база не бачила: {len(gained)}")
for g in gained[:5]:
    print(f"      {g}")
for t, a, b in tick_mismatch[:5]:
    print(f"      «{t}» бектест={a} детект={b}")

# ---------- 3) що саме пішло б у торгівлю ----------
tradeable = [e for e in events
             if bw.classify(e["title"]) == "SPOT_DELIST" and not e.get("margin_only")]
tk = Counter()
for e in tradeable:
    for t in bw.extract_tickers(e["title"]):
        tk[t] += 1
print(f"\n=== 3) ЩО ПІШЛО Б У ТОРГІВЛЮ ===")
print(f"    подій, які тригерять вхід: {len(tradeable)}")
print(f"    унікальних тикерів:        {len(tk)}")
print(f"    тикерів на подію:          {sum(tk.values())/max(len(tradeable),1):.1f}")

# ---------- 4) захист від сміття ----------
print("\n=== 4) ЩО НЕ МАЄ ТРИГЕРИТИ ВХІД ===")
margin = [e for e in events if e.get("margin_only")]
still = [e for e in margin if bw.classify(e["title"]) == "SPOT_DELIST"
         and bw.extract_tickers(e["title"])]
check("margin-анонси не йдуть у SPOT_DELIST", not still,
      f"протекло {len(still)}")
for e in still[:3]:
    print(f"      «{e['title'][:70]}»")

empty = [e for e in tradeable if not bw.extract_tickers(e["title"])]
# «Will Delist All American-Style Daily Options» — це продукт, а не токен;
# порожній список тут ПРАВИЛЬНА поведінка, інакше ми б шортили слово.
opts = [e for e in empty if "option" in e["title"].lower()]
check("порожні тикери лише в опціонних анонсах", len(opts) == len(empty),
      f"порожніх {len(empty)}, з них опціони {len(opts)}")
for e in empty[:3]:
    print(f"      «{e['title'][:70]}»")

# ---------- 5) сміттєві «тикери» ----------
print("\n=== 5) ПІДОЗРІЛІ ТИКЕРИ (можливі хибні спрацювання) ===")
# NB: FOR і THE — СПРАВЖНІ тикери (ForTube, The Protocol), обидва делістились.
# Саме через стоп-лист їх колись і губили, тому в JUNK їх бути не має.
JUNK = {"USDT", "BUSD", "USDC", "SPOT", "AND", "ON", "WILL", "ALL",
        "NEW", "API", "VIP", "UTC", "DELIST", "TRADING", "PAIRS", "OPTIONS"}
bad = sorted(set(tk) & JUNK)
check("серед тикерів нема службових слів", not bad, str(bad))
short = sorted(t for t in tk if len(t) == 1)
print(f"    однолітерні тикери (легальні, напр. D): {short or 'нема'}")
top = ", ".join(f"{t}×{n}" for t, n in tk.most_common(8))
print(f"    найчастіші: {top}")

print("\n" + ("РЕПЛЕЙ OK" if not FAILS else f"ПРОВАЛЕНО: {FAILS}"))
sys.exit(1 if FAILS else 0)
