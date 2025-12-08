# =============================================================================
# Struja Srbija Bot (Telegram) — Render + FastAPI (Webhook)
# -----------------------------------------------------------------------------
# Что внутри:
# • Полная логика бота, хранение в PostgreSQL (kvstore) или локальные файлы (fallback)
# • FastAPI с маршрутами /healthz, /webhook, /set_webhook, /db_ping
# • PTB v20 работает внутри ASGI (webhook)
#
# Переменные окружения (Render):
# TELEGRAM_BOT_TOKEN = <токен бота>
# PUBLIC_URL         = https://<имя-сервиса>.onrender.com
# (опц.) DATA_DIR    = /var/data (иначе ./data)
# (опц.) DATABASE_URL= postgresql://user:pass@host:port/db
# =============================================================================

import asyncio
import os
import json
import hmac
import logging
import calendar
from datetime import datetime
from contextlib import asynccontextmanager, suppress
from http import HTTPStatus
from typing import Optional, Tuple, List
from decimal import Decimal
import uuid
from html import escape
import base64


from fastapi import FastAPI, Request, Response
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# === 0) ХРАНИЛИЩЕ (локальная папка fallback) ===
BASE_PATH = os.environ.get("DATA_DIR", "./data")
os.makedirs(BASE_PATH, exist_ok=True)

READINGS_FP = os.path.join(BASE_PATH, "readings.json")
TARIFFS_FP = os.path.join(BASE_PATH, "tariffs.json")
STATE_FP = os.path.join(BASE_PATH, "state.json")

for fp in (READINGS_FP, TARIFFS_FP, STATE_FP):
    if not os.path.exists(fp):
        with open(fp, "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False, indent=4)


# === 1) JSON утилиты (БД или файлы) ===

def load_json(fp: str) -> dict:
    """
    Простая загрузка JSON из локального файла.
    Никаких Supabase/БД, никаких миграций.
    """
    try:
        with open(fp, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        # файла нет — считаем, что данных пока нет
        return {}
    except json.JSONDecodeError:
        # файл есть, но повреждён — логируем и возвращаем пустой dict
        logging.exception("Не удалось разобрать JSON из файла %s", fp)
        return {}

def save_json(fp: str, data: dict) -> None:
    """
    Сохраняем данные только в локальный файл.
    Запись атомарная: сначала во временный, затем заменяем основной.
    """
    tmp = fp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    os.replace(tmp, fp)


def json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, uuid.UUID):
        return str(o)
    return str(o)


# === 1.2) STATE helpers (сохранённые отчёты) ===
def _load_state() -> dict:
    return load_json(STATE_FP)


def _save_state(state: dict) -> None:
    save_json(STATE_FP, state)


def save_report(ym: str, days: int, text: str):
    state = _load_state()
    reports = state.setdefault("reports", {})
    reports[ym] = {
        "days": int(days),
        "text": text,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    _save_state(state)


def get_saved_report(ym: str) -> Optional[dict]:
    state = _load_state()
    return state.get("reports", {}).get(ym)


# === 2) Доступ и токен ===
ALLOWED_USERS = [1153629050, 5136441143]
ADMIN_ID = 1153629050
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")


# === 3) Даты/утилиты ===
def ym_key_for_month(month: int, now: Optional[datetime] = None) -> str:
    now = now or datetime.now()
    return f"{now.year:04d}-{month:02d}"


def month_name_ru(m: int) -> str:
    names = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
             "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    return names[m - 1]


def month_name_short_ru(m: int) -> str:
    names = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн",
             "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]
    return names[m - 1]


def month_label_mm_yy(year: int, month: int) -> str:
    return f"{month:02d}.{str(year)[2:]}"


def default_days_for_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def find_prev_month_key(readings: dict, floor: str, target_ym: str) -> Optional[str]:
    keys = sorted(readings.keys())
    prev = None
    for k in keys:
        if k < target_ym and floor in readings.get(k, {}):
            prev = k
    return prev


def find_next_month_key(readings: dict, floor: str, target_ym: str) -> Optional[str]:
    keys = sorted(readings.keys())
    for k in keys:
        if k > target_ym and floor in readings.get(k, {}):
            return k
    return None


def readings_monotonic_ok(prev_vals: Optional[dict], new_day: int, new_night: int,
                          next_vals: Optional[dict]) -> Tuple[bool, str]:
    MAX_JUMP = 5000
    if prev_vals:
        if new_day < prev_vals.get("day", 0) or new_night < prev_vals.get("night", 0):
            return False, "Новые показания меньше предыдущих. Проверьте ввод."
        if (new_day - prev_vals.get("day", 0)) > MAX_JUMP or (new_night - prev_vals.get("night", 0)) > MAX_JUMP:
            return False, "Слишком большой скачок показаний (возможна опечатка)."
    if next_vals:
        if new_day > next_vals.get("day", 10**12) or new_night > next_vals.get("night", 10**12):
            return False, "Новые показания больше уже сохранённых в следующем месяце. Нарушается нарастающий итог."
    return True, ""


def split_proportionally(total_green: float, d_day: int, d_night: int) -> Tuple[int, int]:
    total = d_day + d_night
    if total <= 0:
        return 0, 0
    g_day = int(round(total_green * (d_day / total)))
    g_night = int(total_green) - g_day
    g_day = max(0, min(g_day, d_day))
    g_night = max(0, min(g_night, d_night))
    lack = int(total_green) - (g_day + g_night)
    if lack != 0:
        if d_day - g_day >= abs(lack):
            g_day += lack
        elif d_night - g_night >= abs(lack):
            g_night += lack
    return g_day, g_night


def _last_n_months(n: int, ref: Optional[datetime] = None) -> List[Tuple[int, int, str]]:
    ref = ref or datetime.now()
    y, m = ref.year, ref.month
    out = []
    for _ in range(n):
        out.append((y, m, f"{y:04d}-{m:02d}"))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    out.reverse()
    return out


def _delta_for_month(readings: dict, floor: str, year: int, month: int) -> Tuple[Optional[int], Optional[int]]:
    ym = f"{year:04d}-{month:02d}"
    # предыдущий месяц
    py, pm = year, month - 1
    if pm == 0:
        pm = 12
        py -= 1
    prev_ym = f"{py:04d}-{pm:02d}"
    curr = readings.get(ym, {}).get(floor)
    prev = readings.get(prev_ym, {}).get(floor)
    if not curr or not prev:
        return None, None
    # безопасно читаем числа
    try:
        curr_day = int(curr.get("day", 0))
        curr_nt = int(curr.get("night", 0))
        prev_day = int(prev.get("day", 0))
        prev_nt = int(prev.get("night", 0))
    except (TypeError, ValueError):
        return None, None
    d_day = curr_day - prev_day
    d_nt = curr_nt - prev_nt
    if d_day < 0 or d_nt < 0:
        return None, None
    return d_day, d_nt


# === 4) Тарифы ===
def load_tariffs() -> dict:
    data = load_json(TARIFFS_FP)
    versions = data.get("versions", [])
    changed = False
    for i, v in enumerate(versions):
        if "effective_from" not in v:
            v["effective_from"] = "1900-01" if i == 0 else (v.get("created_at", "")[:7] or "1900-01")
            changed = True
    if changed:
        data["versions"] = versions
        save_json(TARIFFS_FP, data)
    if "versions" not in data:
        data["versions"] = []
    return data


def save_tariffs(data: dict) -> None:
    save_json(TARIFFS_FP, data)


def get_latest_tariff_version_id(tariffs: dict) -> Optional[int]:
    versions = tariffs.get("versions", [])
    if not versions:
        return None
    return versions[-1]["id"]


def get_latest_tariff(tariffs: dict) -> Optional[dict]:
    vid = get_latest_tariff_version_id(tariffs)
    if vid is None:
        return None
    return next(v for v in tariffs["versions"] if v["id"] == vid)


def add_tariff_version(tariffs: dict, gd: float, gn: float, bd: float, bn: float,
                       by_user_id: int, currency: str = "RSD") -> int:
    versions = tariffs.setdefault("versions", [])
    new_id = (versions[-1]["id"] + 1) if versions else 1
    versions.append({
        "id": new_id,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "by": by_user_id,
        "currency": currency,
        "green_day": float(gd),
        "green_night": float(gn),
        "blue_day": float(bd),
        "blue_night": float(bn),
        "note": "Version with effective_from; applied to ym >= effective_from"
    })
    return new_id


def get_applicable_tariff(tariffs: dict, ym: str) -> Optional[dict]:
    versions = tariffs.get("versions", [])
    applicable = [v for v in versions if v.get("effective_from", "1900-01") <= ym]
    if not applicable:
        return None
    applicable.sort(key=lambda v: v.get("effective_from", "1900-01"))
    return applicable[-1]


# === 5) Отчёты ===
def build_calc_text_for_month(readings: dict, year: int, month: int, dim_days: int) -> str:
    ym = f"{year:04d}-{month:02d}"
    green_limit_per_floor = dim_days * (350 / 30)

    tariffs_data = load_tariffs()
    tariff_info = get_applicable_tariff(tariffs_data, ym)
    if not tariff_info:
        return (
            f"📊 Расчёт за {month_name_ru(month)} {year}\n\n"
            "❌ Не найдена тарифная версия, применимая к этому месяцу.\n"
            "Добавьте стартовые тарифы или задайте новые с месяцем начала действия."
        )
    currency = tariff_info.get("currency", "RSD")

    lines = [
        f"📊 Расчёт за {month_name_ru(month)} {year}",
        f"Тарифы:",
        f" • Зеленая ВТ: {tariff_info['green_day']} {currency}",
        f" • Зеленая НТ: {tariff_info['green_night']} {currency}",
        f" • Синяя ВТ: {tariff_info['blue_day']} {currency}",
        f" • Синяя НТ: {tariff_info['blue_night']} {currency}",
        "",
        f"Учтено дней: {dim_days} (лимит зелёной зоны на этаж: {int(green_limit_per_floor)} кВт⋅ч)",
        "",
    ]

    totals = {"day": 0, "night": 0, "g_day": 0, "g_night": 0, "b_day": 0, "b_night": 0, "cost": 0.0}

    # Предыдущий месяц
    py, pm = year, month - 1
    if pm == 0:
        pm = 12
        py -= 1
    prev_ym = f"{py:04d}-{pm:02d}"

    for floor in ("1", "2"):
        curr = readings.get(ym, {}).get(floor)
        prev = readings.get(prev_ym, {}).get(floor)
        if not curr:
            lines.append(f"Этаж {floor}: нет показаний за {month_name_ru(month)} {year}.\n")
            continue
        if not prev:
            lines.append(f"Этаж {floor}: нет показаний за {month_name_ru(pm)} {py} — расход не восстановить.\n")
            continue

        d_day = curr.get("day", 0) - prev.get("day", 0)
        d_nt = curr.get("night", 0) - prev.get("night", 0)
        if d_day < 0 or d_nt < 0:
            lines.append(f"Этаж {floor}: обнаружено уменьшение счётчика (ВТ/НТ). Проверьте данные.\n")
            continue

        total = d_day + d_nt
        green_total = int(min(total, green_limit_per_floor))
        g_day, g_night = split_proportionally(green_total, d_day, d_nt)
        b_day = d_day - g_day
        b_night = d_nt - g_night

        cost_green = g_day * tariff_info["green_day"] + g_night * tariff_info["green_night"]
        cost_blue = b_day * tariff_info["blue_day"] + b_night * tariff_info["blue_night"]
        cost_total = cost_green + cost_blue

        totals["day"] += d_day
        totals["night"] += d_nt
        totals["g_day"] += g_day
        totals["g_night"] += g_night
        totals["b_day"] += b_day
        totals["b_night"] += b_night
        totals["cost"] += cost_total

        lines += [
            f"— Этаж {floor}",
            f" • Потребление: ВТ {d_day} кВт⋅ч, НТ {d_nt} кВт⋅ч (итого {total})",
            f" • Зелёная зона: ВТ {g_day}, НТ {g_night} (итого {g_day + g_night})",
            f" • Синяя зона: ВТ {b_day}, НТ {b_night} (итого {b_day + b_night})",
            f" • Стоимость: {cost_total:.2f} {currency}",
            "",
        ]

    h_total = totals["day"] + totals["night"]
    g_total = totals["g_day"] + totals["g_night"]
    b_total = totals["b_day"] + totals["b_night"]

    lines += [
        "🏠 Итог по дому:",
        f" • Потребление: ВТ {totals['day']}, НТ {totals['night']} (итого {h_total})",
        f" • Зелёная зона: ВТ {totals['g_day']}, НТ {totals['g_night']} (итого {g_total})",
        f" • Синяя зона: ВТ {totals['b_day']}, НТ {totals['b_night']} (итого {b_total})",
        f" • 💰 Общая стоимость: {totals['cost']:.2f} {currency}",
    ]
    return "\n".join(lines)


def build_stats_last12_table_for_floor(readings: dict, floor: str) -> str:
    months = _last_n_months(12)
    header = ["Мес", "ВТ", "НТ", "Итого"]
    widths = [5, 6, 6, 6]

    def fmt_cell(val, w):
        s = "—" if val is None else str(val)
        return s.rjust(w)

    head = " ".join(h.ljust(w) for h, w in zip(header, widths))
    lines = [head, "-" * len(head)]
    shown_rows = 0

    for (year, month, _ym) in months:
        d_day, d_nt = _delta_for_month(readings, floor, year, month)
        if d_day is None and d_nt is None:
            continue
        total = ((d_day or 0) + (d_nt or 0))
        if total > 0:
            p_day = round((d_day or 0) * 100 / total)
            p_nt = round((d_nt or 0) * 100 / total)
        else:
            p_day = p_nt = None

        mlabel = month_label_mm_yy(year, month)
        row_sum = [
            mlabel.ljust(widths[0]),
            fmt_cell(d_day, widths[1]),
            fmt_cell(d_nt, widths[2]),
            fmt_cell(total if (d_day is not None or d_nt is not None) else None, widths[3]),
        ]
        lines.append(" ".join(row_sum))

        row_pct = [
            " ".ljust(widths[0]),
            fmt_cell(f"{p_day}%" if p_day is not None else None, widths[1]),
            fmt_cell(f"{p_nt}%" if p_nt is not None else None, widths[2]),
            " ".rjust(widths[3]),
        ]
        lines.append(" ".join(row_pct))
        shown_rows += 1

    if shown_rows == 0:
        return "Нет достаточных данных для построения таблицы."

    lines.append("-" * len(head))
    return "\n".join(lines)


def build_readings_table_for_floor(readings: dict, floor: str) -> str:
    months = sorted([k for k in readings.keys() if k != "_meta"], reverse=True)
    header = ["Мес", "ВТ", "НТ"]
    widths = [5, 7, 7]
    head = " ".join(h.ljust(w) for h, w in zip(header, widths))
    lines = [head, "-" * len(head)]
    for ym in months:
        y, m = int(ym[:4]), int(ym[5:])
        vals = readings.get(ym, {}).get(floor, {})
        d_day = vals.get("day") if vals else None
        d_nt = vals.get("night") if vals else None
        row = [
            month_label_mm_yy(y, m).ljust(widths[0]),
            ("—" if d_day is None else str(d_day)).rjust(widths[1]),
            ("—" if d_nt is None else str(d_nt)).rjust(widths[2]),
        ]
        lines.append(" ".join(row))
    if len(lines) == 2:
        return "Нет достаточных данных для построения таблицы."
    return "\n".join(lines)


def _readings_payload(raw: dict) -> dict:
    if isinstance(raw, dict) and isinstance(raw.get("readings"), dict):
        return raw["readings"]
    return raw


def _latest_month(readings: dict) -> Optional[str]:
    months = [k for k in readings.keys() if k != "_meta" and isinstance(readings.get(k), dict)]
    if not months:
        return None
    return sorted(months)[-1]


def _format_month_human(ym: str) -> str:
    try:
        year = int(ym[:4])
        month = int(ym[5:7])
    except (ValueError, TypeError):
        return ym
    return f"{month_name_ru(month)} {year}"


def build_submission_card_html(raw_readings: dict) -> str:
    readings = _readings_payload(raw_readings)
    latest = _latest_month(readings)
    month_label = _format_month_human(latest) if latest else "—"
    month_data = readings.get(latest, {}) if latest else {}

    meters = {
        "1": "501000021651",
        "2": "501000021652",
    }

    def fmt_val(val: Optional[int]) -> str:
        if val is None:
            return "—"
        return str(val)

    lines = []
    for floor in ("1", "2"):
        vals = month_data.get(floor, {}) if isinstance(month_data, dict) else {}
        day_val = vals.get("day") if isinstance(vals, dict) else None
        night_val = vals.get("night") if isinstance(vals, dict) else None
        meter = meters.get(floor, floor)
        lines.append(f"Шифра м. м. - {meter}")
        lines.append(
            f"ВТ: {fmt_val(day_val)}    НТ: {fmt_val(night_val)}"
        )
        lines.append("")

    text_block = "\n".join(lines).rstrip()

    parts = [
        "<b>Аранджеловац</b>",
        f"Мес.: {escape(month_label)}",
        "",
        "Показания",
        f"<pre>{escape(text_block)}</pre>",
        "",
        "<b>Ябланичка</b>",
        "ЕД Број: 278&#8203;067&#8203;621",
    ]
    return "\n".join(parts)


# === 6) Поиск последнего полного месяца ===
def latest_complete_month(readings: dict) -> Optional[str]:
    months = sorted([k for k in readings.keys() if k != "_meta"])
    latest = None
    for ym in months:
        y, m = int(ym[:4]), int(ym[5:])
        py, pm = y, m - 1
        if pm == 0:
            pm = 12
            py -= 1
        prev_ym = f"{py:04d}-{pm:02d}"
        this_ok = "1" in readings.get(ym, {}) and "2" in readings.get(ym, {})
        prev_ok = "1" in readings.get(prev_ym, {}) and "2" in readings.get(prev_ym, {})
        if this_ok and prev_ok:
            latest = ym
    return latest


def tariff_exists_for_month(ym: str) -> bool:
    tariffs = load_tariffs()
    return get_applicable_tariff(tariffs, ym) is not None


# === 7) Состояния диалогов ===
(CHOOSE_FLOOR, CHOOSE_MONTH, ENTER_DAY, ENTER_NIGHT,
 CONFIRM_OVERWRITE, CONFIRM_SAVE) = range(6)

(T_ENTER_GD, T_ENTER_GN, T_ENTER_BD, T_ENTER_BN,
 T_ENTER_EFF, T_CONFIRM_TAR) = range(100, 106)

(CALC_ENTER_DAYS, CALC_CONFIRM_DAYS) = range(200, 202)
(CALC_CHOOSE_ACTION,) = range(202, 203)

(PREV_CHOOSE_MONTH,) = range(220, 221)
(UNDO_CHOOSE_FLOOR, UNDO_CONFIRM) = range(300, 302)

AFTER_SAVE_PROMPT = 400
(STATS_CHOOSE_TYPE, STATS_CONS_CHOOSE_FLOOR, STATS_READ_CHOOSE_FLOOR) = range(500, 503)


# === 8) Меню (c админ-кнопкой) ===
def is_admin_id(user_id: Optional[int]) -> bool:
    return user_id == ADMIN_ID


def get_main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📥 Ввести показания", callback_data="input_readings")],
        [InlineKeyboardButton("📊 Новый расчёт за месяц", callback_data="calc_current")],
        [InlineKeyboardButton("📅 Показать прежние расчеты", callback_data="show_prev")],
        [InlineKeyboardButton("🪪 Карточка подачи показаний", callback_data="card_submission")],
        [InlineKeyboardButton("💰 Изменить тарифы", callback_data="set_tariffs")],
        [InlineKeyboardButton("↩️ Откат последних показаний", callback_data="undo_last")],
        [InlineKeyboardButton("📈 Статистика", callback_data="stats_menu")],
    ]
    # Админ-кнопка
    if is_admin:
        keyboard.append(
            [InlineKeyboardButton("адм: Просмотр таблиц", callback_data="admin_show_tables")]
        )
    return InlineKeyboardMarkup(keyboard)


def card_submission_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "Позвонить 0800 360 300 (ЕДС)",
                callback_data="call_eds",
            )
        ],
        [InlineKeyboardButton("↩️ В меню", callback_data="back_menu")],
    ])


def main_menu_markup_for(update: Update) -> InlineKeyboardMarkup:
    uid = update.effective_user.id if update and update.effective_user else None
    return get_main_menu(is_admin=is_admin_id(uid))


# === 9) Клавиатуры ===
def floors_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1 этаж", callback_data="floor_1"),
         InlineKeyboardButton("2 этаж", callback_data="floor_2")],
        [InlineKeyboardButton("↩️ В меню", callback_data="back_menu")],
    ])


def months_kb() -> InlineKeyboardMarkup:
    now = datetime.now()
    buttons, row = [], []
    for m in range(1, 13):
        row.append(InlineKeyboardButton(f"{month_name_ru(m)} {now.year}", callback_data=f"month_{m}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_floor"),
                    InlineKeyboardButton("↩️ В меню", callback_data="back_menu")])
    return InlineKeyboardMarkup(buttons)


def prev_months_kb(readings: dict) -> InlineKeyboardMarkup:
    now_ym = datetime.now().strftime("%Y-%m")
    months = sorted(k for k in readings.keys() if k != "_meta" and k < now_ym)
    months = months[-12:]
    buttons, row = [], []
    for ym in months:
        row.append(InlineKeyboardButton(ym, callback_data=f"prev_show_{ym}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    if not buttons:
        buttons = [[InlineKeyboardButton("Нет доступных месяцев", callback_data="noop")]]
    buttons.append([InlineKeyboardButton("↩️ В меню", callback_data="back_menu")])
    return InlineKeyboardMarkup(buttons)


def back_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад к выбору месяца", callback_data="back_month")],
        [InlineKeyboardButton("↩️ В меню", callback_data="back_menu")],
    ])


def confirm_overwrite_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Перезаписать", callback_data="ow_yes"),
                                  InlineKeyboardButton("Отмена", callback_data="ow_no")]])


def confirm_save_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Сохранить", callback_data="save_yes"),
                                  InlineKeyboardButton("Отмена", callback_data="save_no")]])


def tariffs_back_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("↩️ В меню", callback_data="back_menu")]])


def tariffs_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Сохранить тарифы", callback_data="tar_save_yes"),
                                  InlineKeyboardButton("Отмена", callback_data="tar_save_no")]])


def calc_back_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("↩️ В меню", callback_data="back_menu")]])


def calc_days_choice_kb(suggest_days: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Использовать {suggest_days} дней", callback_data=f"use_days_{suggest_days}")],
        [InlineKeyboardButton("✏️ Ввести вручную", callback_data="enter_days_manual")],
        [InlineKeyboardButton("↩️ В меню", callback_data="back_menu")],
    ])


def ask_other_floor_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Да, внести", callback_data="add_other_yes")],
        [InlineKeyboardButton("Нет, в меню", callback_data="add_other_no")]
    ])


def undo_confirm_kb(latest_ym: str, vals: dict, floor: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Да, удалить",
                                                       callback_data=f"undo_yes_{latest_ym}_{floor}"),
                                  InlineKeyboardButton("❌ Отмена", callback_data="undo_no")]])


def stats_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Потребление по месяцам", callback_data="stats_cons")],
        [InlineKeyboardButton("Показания по месяцам", callback_data="stats_read")],
        [InlineKeyboardButton("↩️ В меню", callback_data="back_menu")],
    ])


def stats_floors_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Этаж 1", callback_data=f"{prefix}_floor_1"),
         InlineKeyboardButton("Этаж 2", callback_data=f"{prefix}_floor_2")],
        [InlineKeyboardButton("↩️ В меню", callback_data="back_menu")],
    ])


def calc_saved_choice_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Показать сохранённый расчёт", callback_data="calc_view_saved")],
        [InlineKeyboardButton("🔁 Пересчитать заново", callback_data="calc_recalc")],
        [InlineKeyboardButton("↩️ В меню", callback_data="back_menu")],
    ])


# --- утилита: безопасно резать длинные сообщения под Telegram 4096 ---
def send_long_text(_unused, text: str):
    MAX_LEN = 4096
    if len(text) <= MAX_LEN:
        return [text]
    parts, buf = [], []
    cur_len = 0
    for line in text.splitlines(True):
        if cur_len + len(line) > MAX_LEN:
            parts.append("".join(buf))
            buf, cur_len = [], 0
        buf.append(line)
        cur_len += len(line)
    if buf:
        parts.append("".join(buf))
    return parts


# === 9.1) Хендлеры: старт/меню ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in ALLOWED_USERS:
        if update.message:
            await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return
    if update.message:
        await update.message.reply_text("Добро пожаловать! Выберите действие:",
                                        reply_markup=main_menu_markup_for(update))


async def open_menu_from_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.",
                                  reply_markup=main_menu_markup_for(update))
        return
    if q.data == "input_readings":
        await q.edit_message_text("Выберите этаж:", reply_markup=floors_kb())
        return CHOOSE_FLOOR
    await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))


async def card_submission_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text(
            "⛔ У вас нет доступа к этому боту.",
            reply_markup=main_menu_markup_for(update),
        )
        return
    readings = load_json(READINGS_FP)
    html = build_submission_card_html(readings)
    await q.edit_message_text(
        html,
        reply_markup=card_submission_kb(),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def card_submission_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text(
            "⛔ У вас нет доступа к этому боту.",
            reply_markup=main_menu_markup_for(update),
        )
        return

    phone_number = "+381800360300"
    if q.message:
        with suppress(TelegramError):
            await q.message.reply_contact(
                phone_number=phone_number,
                first_name="ЕДС",
                last_name="Контакт-центр 0800 360 300",
            )
        with suppress(TelegramError):
            await q.message.edit_reply_markup(reply_markup=card_submission_kb())
    else:
        with suppress(TelegramError):
            await context.bot.send_contact(
                chat_id=update.effective_chat.id,
                phone_number=phone_number,
                first_name="ЕДС",
                last_name="Контакт-центр 0800 360 300",
            )


# --- Админ: информация о хранилище ---
async def admin_db_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id

    # доступ только админу
    if not is_admin_id(uid):
        await q.edit_message_text(
            "🚫 Доступ запрещён.",
            reply_markup=main_menu_markup_for(update),
        )
        return

    # Обновляем текущее сообщение – краткая информация + главное меню
    info_lines = [
        "ℹ️ Бот работает *только* с локальными файлами данных.",
        "",
        f"📁 Каталог данных: `{BASE_PATH}`",
        "",
        "Сейчас выведу содержимое трёх файлов:",
        "• `readings.json`  — показания счётчиков",
        "• `tariffs.json`   — версии тарифов",
        "• `state.json`     — сохранённые отчёты и состояние",
        "",
        "Если файлы большие, они будут показаны в нескольких сообщениях.",
    ]
    await q.edit_message_text(
        "\n".join(info_lines),
        reply_markup=main_menu_markup_for(update),
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )

    chat_id = update.effective_chat.id

    # Вспомогательная функция: вывести один JSON-файл частями
    async def send_json_file(title: str, fp: str):
        data = load_json(fp)
        # заголовок + pretty-printed JSON
        header = f"📄 {title} (`{os.path.basename(fp)}`)\n\n"
        raw_json = json.dumps(data, ensure_ascii=False, indent=2)
        text_block = header + raw_json

        # экранируем под HTML и режем на части, чтобы не упереться в лимит 4096
        pre_content = escape(text_block)
        for part in send_long_text(None, pre_content):
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"<pre>{part}</pre>",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )

    # Показываем три файла по очереди
    await send_json_file("Показания счётчиков", READINGS_FP)
    await send_json_file("Тарифы", TARIFFS_FP)
    await send_json_file("Состояние / отчёты", STATE_FP)




# === Ввод показаний и прочие сценарии ===
async def choose_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if q.data not in {"floor_1", "floor_2"}:
        await q.edit_message_text("Выберите этаж:", reply_markup=floors_kb())
        return CHOOSE_FLOOR
    floor = "1" if q.data.endswith("_1") else "2"
    context.user_data["floor"] = floor
    await q.edit_message_text(f"Этаж: {floor}\nТеперь выберите месяц:", reply_markup=months_kb())
    return CHOOSE_MONTH


async def choose_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "back_floor":
        await q.edit_message_text("Выберите этаж:", reply_markup=floors_kb())
        return CHOOSE_FLOOR
    if q.data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if not q.data.startswith("month_"):
        await q.edit_message_text("Выберите месяц:", reply_markup=months_kb())
        return CHOOSE_MONTH
    month = int(q.data.split("_")[1])
    context.user_data["month"] = month
    await q.edit_message_text(
        f"Этаж: {context.user_data['floor']}\n"
        f"Месяц: {month_name_ru(month)} {datetime.now().year}\n\n"
        f"Введите показания ВТ:",
        reply_markup=back_cancel_kb(),
    )
    return ENTER_DAY


async def enter_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ALLOWED_USERS:
        return ConversationHandler.END
    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.message.reply_text("Пожалуйста, введите целое число для ВТ.", reply_markup=back_cancel_kb())
        return ENTER_DAY
    context.user_data["day_val"] = int(text)
    await update.message.reply_text("Теперь введите показания НТ:", reply_markup=back_cancel_kb())
    return ENTER_NIGHT


async def enter_night(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ALLOWED_USERS:
        return ConversationHandler.END
    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.message.reply_text("Пожалуйста, введите целое число для НТ.", reply_markup=back_cancel_kb())
        return ENTER_NIGHT
    context.user_data["night_val"] = int(text)

    readings = load_json(READINGS_FP)
    floor = context.user_data["floor"]
    month = context.user_data["month"]
    ym = ym_key_for_month(month)

    exists = ym in readings and floor in readings.get(ym, {})

    prev_key = find_prev_month_key(readings, floor, ym)
    next_key = find_next_month_key(readings, floor, ym)
    prev_vals = readings.get(prev_key, {}).get(floor) if prev_key else None
    next_vals = readings.get(next_key, {}).get(floor) if next_key else None

    ok, msg = readings_monotonic_ok(prev_vals,
                                    context.user_data["day_val"],
                                    context.user_data["night_val"],
                                    next_vals)
    if not ok:
        await update.message.reply_text(f"⚠️ {msg}\n\nПовторите ввод ВТ:", reply_markup=back_cancel_kb())
        return ENTER_DAY

    if exists:
        await update.message.reply_text(
            f"За {month_name_ru(month)} {datetime.now().year} для этажа {floor} уже есть показания.\n"
            f"Перезаписать?\n\n"
            f"Новые ВТ/НТ: {context.user_data['day_val']} / {context.user_data['night_val']}",
            reply_markup=confirm_overwrite_kb(),
        )
        return CONFIRM_OVERWRITE

    await update.message.reply_text(
        "Подтвердите сохранение:\n"
        f"Этаж: {floor}\n"
        f"Месяц: {month_name_ru(month)} {datetime.now().year}\n"
        f"ВТ: {context.user_data['day_val']}\n"
        f"НТ: {context.user_data['night_val']}",
        reply_markup=confirm_save_kb(),
    )
    return CONFIRM_SAVE


async def confirm_overwrite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    floor = context.user_data["floor"]
    month = context.user_data["month"]
    ym = ym_key_for_month(month)

    if q.data == "ow_no":
        await q.edit_message_text("Отменено. Возврат в меню.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END

    readings = load_json(READINGS_FP)
    readings.setdefault(ym, {})
    readings[ym][floor] = {"day": context.user_data["day_val"], "night": context.user_data["night_val"]}
    save_json(READINGS_FP, readings)

    other_floor = "2" if floor == "1" else "1"
    if other_floor not in readings.get(ym, {}):
        await q.edit_message_text(
            "✅ Сохранено (перезаписано).\n"
            f"Этаж: {floor}\n"
            f"{month_name_ru(month)} {datetime.now().year}\n"
            f"ВТ/НТ: {context.user_data['day_val']} / {context.user_data['night_val']}\n\n"
            f"Хотите сейчас внести показания для этажа {other_floor} за {month_name_ru(month)} {datetime.now().year}?",
            reply_markup=ask_other_floor_kb(),
        )
        return AFTER_SAVE_PROMPT

    await q.edit_message_text("✅ Сохранено (перезаписано).", reply_markup=main_menu_markup_for(update))
    return ConversationHandler.END


async def confirm_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    floor = context.user_data["floor"]
    month = context.user_data["month"]
    ym = ym_key_for_month(month)

    if q.data == "save_no":
        await q.edit_message_text("Отменено. Возврат в меню.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END

    readings = load_json(READINGS_FP)
    readings.setdefault(ym, {})
    readings[ym][floor] = {"day": context.user_data["day_val"], "night": context.user_data["night_val"]}
    save_json(READINGS_FP, readings)

    other_floor = "2" if floor == "1" else "1"
    if other_floor not in readings.get(ym, {}):
        await q.edit_message_text(
            "✅ Сохранено.\n"
            f"Этаж: {floor}\n"
            f"{month_name_ru(month)} {datetime.now().year}\n"
            f"ВТ/НТ: {context.user_data['day_val']} / {context.user_data['night_val']}\n\n"
            f"Хотите ли сейчас еще внести показания для этажа {other_floor} за {month_name_ru(month)} {datetime.now().year}?",
            reply_markup=ask_other_floor_kb(),
        )
        return AFTER_SAVE_PROMPT

    await q.edit_message_text("✅ Сохранено.", reply_markup=main_menu_markup_for(update))
    return ConversationHandler.END


async def after_save_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "add_other_no":
        await q.edit_message_text("Ок, возвращаю в меню.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if data == "add_other_yes":
        current_floor = context.user_data.get("floor")
        other_floor = "2" if current_floor == "1" else "1"
        context.user_data["floor"] = other_floor
        month = context.user_data.get("month")
        await q.edit_message_text(
            f"Этаж: {other_floor}\nМесяц: {month_name_ru(month)} {datetime.now().year}\n\n"
            f"Введите показания ВТ:",
            reply_markup=back_cancel_kb(),
        )
        return ENTER_DAY
    await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
    return ConversationHandler.END


async def back_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if q.data == "back_floor":
        await q.edit_message_text("Выберите этаж:", reply_markup=floors_kb())
        return CHOOSE_FLOOR
    if q.data == "back_month":
        await q.edit_message_text("Выберите месяц:", reply_markup=months_kb())
        return CHOOSE_MONTH


# === Изменение тарифов ===
def _parse_float(text: str) -> Optional[float]:
    try:
        return float(text.replace(",", "."))
    except Exception:
        return None


def _valid_ym(s: str) -> bool:
    try:
        if len(s) != 7 or s[4] != "-":
            return False
        y = int(s[:4])
        m = int(s[5:])
        return 1900 <= y <= 2100 and 1 <= m <= 12
    except Exception:
        return False


async def tariffs_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END

    current = get_latest_tariff(load_tariffs())
    cur_text = (
        "\n\nТекущие тарифы (последняя добавленная версия):\n"
        f" • Зеленая ВТ: {current['green_day']} RSD\n"
        f" • Зеленая НТ: {current['green_night']} RSD\n"
        f" • Синяя ВТ: {current['blue_day']} RSD\n"
        f" • Синяя НТ: {current['blue_night']} RSD\n"
        f" • Применяется с: {current.get('effective_from','—')}\n"
        if current else "\n\nТекущих тарифов ещё нет."
    )
    await q.edit_message_text(
        "Введите значение для Зеленая ВТ (RSD/кВт⋅ч, число, точка как разделитель):" + cur_text,
        reply_markup=tariffs_back_menu_kb(),
    )
    return T_ENTER_GD


async def tariffs_enter_gd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = _parse_float(update.message.text.strip())
    if val is None or val < 0:
        await update.message.reply_text("Введите неотрицательное число. Повторите ввод Зеленая ВТ:",
                                        reply_markup=tariffs_back_menu_kb())
    else:
        context.user_data["tar_gd"] = val
        await update.message.reply_text("Введите значение для Зеленая НТ:", reply_markup=tariffs_back_menu_kb())
        return T_ENTER_GN
    return T_ENTER_GD


async def tariffs_enter_gn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = _parse_float(update.message.text.strip())
    if val is None or val < 0:
        await update.message.reply_text("Введите неотрицательное число. Повторите ввод Зеленая НТ:",
                                        reply_markup=tariffs_back_menu_kb())
    else:
        context.user_data["tar_gn"] = val
        await update.message.reply_text("Введите значение для Синяя ВТ:", reply_markup=tariffs_back_menu_kb())
        return T_ENTER_BD
    return T_ENTER_GN


async def tariffs_enter_bd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = _parse_float(update.message.text.strip())
    if val is None or val < 0:
        await update.message.reply_text("Введите неотрицательное число. Повторите ввод Синяя ВТ:",
                                        reply_markup=tariffs_back_menu_kb())
    else:
        context.user_data["tar_bd"] = val
        await update.message.reply_text("Введите значение для Синяя НТ:", reply_markup=tariffs_back_menu_kb())
        return T_ENTER_BN
    return T_ENTER_BD


async def tariffs_enter_bn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = _parse_float(update.message.text.strip())
    if val is None or val < 0:
        await update.message.reply_text("Введите неотрицательное число. Повторите ввод Синяя НТ:",
                                        reply_markup=tariffs_back_menu_kb())
    else:
        context.user_data["tar_bn"] = val
        await update.message.reply_text(
            "Укажите месяц, с которого применять НОВЫЕ тарифы.\n"
            "Формат: YYYY-MM (например, 2024-07)\n\n"
            "Новые тарифы будут применяться ко всем расчётам, начиная с этого месяца и далее.",
            reply_markup=tariffs_back_menu_kb(),
        )
        return T_ENTER_EFF
    return T_ENTER_BN


async def tariffs_enter_effective_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    eff = (update.message.text or "").strip()
    if not _valid_ym(eff):
        await update.message.reply_text("Некорректный формат. Введите месяц в формате YYYY-MM (например, 2024-07):",
                                        reply_markup=tariffs_back_menu_kb())
        return T_ENTER_EFF

    context.user_data["tar_eff"] = eff
    gd = context.user_data["tar_gd"]; gn = context.user_data["tar_gn"]
    bd = context.user_data["tar_bd"]; bn = context.user_data["tar_bn"]

    await update.message.reply_text(
        "Подтвердите сохранение НОВОЙ версии тарифов:\n"
        f" • Зеленая ВТ: {gd} RSD\n"
        f" • Зеленая НТ: {gn} RSD\n"
        f" • Синяя ВТ: {bd} RSD\n"
        f" • Синяя НТ: {bn} RSD\n"
        f" • Применять начиная с: {eff}\n",
        reply_markup=tariffs_confirm_kb(),
    )
    return T_CONFIRM_TAR


async def tariffs_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "tar_save_no":
        await q.edit_message_text("Отменено. Возврат в меню.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END

    gd = context.user_data.get("tar_gd"); gn = context.user_data.get("tar_gn")
    bd = context.user_data.get("tar_bd"); bn = context.user_data.get("tar_bn")
    eff = context.user_data.get("tar_eff")
    if not (gd is not None and gn is not None and bd is not None and bn is not None and eff):
        await q.edit_message_text("Недостаточно данных для сохранения тарифов.",
                                  reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END

    tariffs = load_tariffs()
    new_id = add_tariff_version(tariffs, gd, gn, bd, bn, by_user_id=update.effective_user.id, currency="RSD")
    for v in tariffs["versions"]:
        if v["id"] == new_id:
            v["effective_from"] = eff
            break
    save_tariffs(tariffs)

    await q.edit_message_text(
        f"✅ Новая версия тарифов сохранена (ID: {new_id}).\n"
        f"Будет применяться начиная с {eff} ко всем последующим месяцам.\n"
        "Файл: data/tariffs.json",
        reply_markup=main_menu_markup_for(update),
    )
    return ConversationHandler.END


# === Расчёт текущего (с предупреждением о ранее сохранённом отчёте) ===
async def calc_current_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END

    readings = load_json(READINGS_FP)
    ym = latest_complete_month(readings)
    if not ym or not tariff_exists_for_month(ym):
        await q.edit_message_text(
            "❌ Нет месяца, по которому можно выполнить расчёт: нет полных показаний (оба этажа здесь и в предыдущем месяце) "
            "или отсутствуют применимые тарифы.\n\n"
            "Сначала внесите показания / добавьте тарифы.",
            reply_markup=main_menu_markup_for(update),
        )
        return ConversationHandler.END

    year, month = int(ym[:4]), int(ym[5:])
    context.user_data["calc_year"] = year
    context.user_data["calc_month"] = month
    context.user_data["calc_ym"] = ym

    saved = get_saved_report(ym)
    if saved:
        await q.edit_message_text(
            f"За {month_name_ru(month)} {year} уже есть сохранённый расчёт на {saved['days']} дней.\n"
            f"Что хотите сделать?",
            reply_markup=calc_saved_choice_kb(),
        )
        return CALC_CHOOSE_ACTION

    suggest = default_days_for_month(year, month)
    await q.edit_message_text(
        f"Последний месяц с внесенными показаниями: {month_name_ru(month)} {year}\n"
        f"Обычно в этом месяце {suggest} дня/дней.\n"
        f"Хотите использовать {suggest}, или ввести своё число дней?",
        reply_markup=calc_days_choice_kb(suggest),
    )
    return CALC_CONFIRM_DAYS


async def calc_saved_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    ym = context.user_data.get("calc_ym")
    year = context.user_data.get("calc_year")
    month = context.user_data.get("calc_month")

    if q.data == "calc_view_saved":
        saved = get_saved_report(ym)
        if not saved:
            await q.edit_message_text("Сохранённый отчёт не найден. Пересчитайте, пожалуйста.",
                                      reply_markup=calc_back_menu_kb())
            return CALC_CONFIRM_DAYS
        await q.edit_message_text(saved["text"], reply_markup=main_menu_markup_for(update),
                                  disable_web_page_preview=True)
        return ConversationHandler.END

    if q.data == "calc_recalc":
        suggest = default_days_for_month(year, month)
        await q.edit_message_text(
            f"Пересчёт за {month_name_ru(month)} {year}. "
            f"Обычно в этом месяце {suggest} дня/дней.\n"
            f"Хотите использовать {suggest}, или ввести своё число дней?",
            reply_markup=calc_days_choice_kb(suggest),
        )
        return CALC_CONFIRM_DAYS

    await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
    return ConversationHandler.END


async def calc_days_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if data == "enter_days_manual":
        await q.edit_message_text("Введите число дней:", reply_markup=calc_back_menu_kb())
        return CALC_ENTER_DAYS
    if data.startswith("use_days_"):
        try:
            dim_days = int(data.split("_")[2])
        except Exception:
            await q.edit_message_text("Не удалось определить число дней. Введите вручную:",
                                      reply_markup=calc_back_menu_kb())
            return CALC_ENTER_DAYS
        month = context.user_data.get("calc_month")
        year = context.user_data.get("calc_year")
        ym = context.user_data.get("calc_ym")
        readings = load_json(READINGS_FP)
        report = build_calc_text_for_month(readings, year, month, dim_days)
        save_report(ym, dim_days, report)
        await q.edit_message_text(report, reply_markup=main_menu_markup_for(update),
                                  disable_web_page_preview=True)
        return ConversationHandler.END

    await q.edit_message_text("Пожалуйста, выберите действие:", reply_markup=calc_back_menu_kb())
    return CALC_CONFIRM_DAYS


async def calc_enter_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.message.reply_text("Введите число дней:", reply_markup=calc_back_menu_kb())
        return CALC_ENTER_DAYS
    dim_days = int(text)
    if dim_days <= 0:
        await update.message.reply_text("Введите число дней:", reply_markup=calc_back_menu_kb())
        return CALC_ENTER_DAYS

    month = context.user_data.get("calc_month")
    year = context.user_data.get("calc_year")
    ym = context.user_data.get("calc_ym")
    readings = load_json(READINGS_FP)
    report = build_calc_text_for_month(readings, year, month, dim_days)
    save_report(ym, dim_days, report)
    await update.message.reply_text(report, reply_markup=main_menu_markup_for(update),
                                    disable_web_page_preview=True)
    return ConversationHandler.END


# === Показ предыдущего месяца ===
async def show_prev_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    readings = load_json(READINGS_FP)
    await q.edit_message_text("Выберите один из предыдущих месяцев:", reply_markup=prev_months_kb(readings))
    return PREV_CHOOSE_MONTH


async def show_prev_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if data == "noop":
        await q.edit_message_text("Нет предыдущих месяцев для показа.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if not data.startswith("prev_show_"):
        await q.edit_message_text("Пожалуйста, выберите месяц:", reply_markup=calc_back_menu_kb())
        return PREV_CHOOSE_MONTH

    ym = data.replace("prev_show_", "")
    try:
        year = int(ym[:4]); month = int(ym[5:])
    except Exception:
        await q.edit_message_text("Некорректный месяц.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END

    saved = get_saved_report(ym)
    if saved:
        await q.edit_message_text(saved["text"], reply_markup=main_menu_markup_for(update),
                                  disable_web_page_preview=True)
        return ConversationHandler.END

    dim_days = default_days_for_month(year, month)
    readings = load_json(READINGS_FP)
    report = build_calc_text_for_month(readings, year, month, dim_days)
    await q.edit_message_text(report, reply_markup=main_menu_markup_for(update),
                              disable_web_page_preview=True)
    return ConversationHandler.END


# === Откат последних показаний ===
def latest_month_for_floor(readings: dict, floor: str) -> Optional[str]:
    months = sorted([ym for ym, v in readings.items() if floor in v and ym != "_meta"])
    return months[-1] if months else None


async def undo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    await q.edit_message_text("Выберите этаж для отката показаний:", reply_markup=floors_kb())
    return UNDO_CHOOSE_FLOOR


async def undo_choose_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    floor = "1" if q.data.endswith("_1") else "2"
    readings = load_json(READINGS_FP)
    latest_ym = latest_month_for_floor(readings, floor)
    if not latest_ym:
        await q.edit_message_text(f"Для этажа {floor} нет данных для отката.",
                                  reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    vals = readings.get(latest_ym, {}).get(floor, {})
    await q.edit_message_text(
        f"Удалить ПОСЛЕДНИЕ показания для этажа {floor} за {latest_ym}?\n"
        f"ВТ: {vals.get('day')}, НТ: {vals.get('night')}",
        reply_markup=undo_confirm_kb(latest_ym, vals, floor),
    )
    return UNDO_CONFIRM


async def undo_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "undo_no":
        await q.edit_message_text("Откат отменён.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if not q.data.startswith("undo_yes_"):
        await q.edit_message_text("Некорректный запрос отката.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    _, _, ym, floor = q.data.split("_")
    readings = load_json(READINGS_FP)
    if ym in readings and floor in readings[ym]:
        del readings[ym][floor]
        remaining_non_meta = [k for k in readings[ym].keys() if k != "_meta"]
        if len(remaining_non_meta) == 0:
            readings.pop(ym)
        save_json(READINGS_FP, readings)
        await q.edit_message_text(
            f"✅ Показания за {ym} для этажа {floor} удалены.\n"
            "Если это был последний этаж в месяце — месяц удалён полностью.",
            reply_markup=main_menu_markup_for(update),
        )
    else:
        await q.edit_message_text("❌ Данных для удаления не найдено.", reply_markup=main_menu_markup_for(update))
    return ConversationHandler.END


# === Статистика ===
async def stats_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    await q.edit_message_text("Выберите раздел статистики:", reply_markup=stats_main_kb())
    return STATS_CHOOSE_TYPE


async def stats_choose_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if data == "stats_cons":
        await q.edit_message_text("Выберите этаж:", reply_markup=stats_floors_kb("stats_cons"))
        return STATS_CONS_CHOOSE_FLOOR
    if data == "stats_read":
        await q.edit_message_text("Выберите этаж:", reply_markup=stats_floors_kb("stats_read"))
        return STATS_READ_CHOOSE_FLOOR
    await q.edit_message_text("Выберите раздел статистики:", reply_markup=stats_main_kb())
    return STATS_CHOOSE_TYPE


async def stats_cons_choose_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if data not in {"stats_cons_floor_1", "stats_cons_floor_2"}:
        await q.edit_message_text("Пожалуйста, выберите этаж:", reply_markup=stats_floors_kb("stats_cons"))
        return STATS_CONS_CHOOSE_FLOOR
    floor = "1" if data.endswith("_1") else "2"
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    readings = load_json(READINGS_FP)
    table_text = build_stats_last12_table_for_floor(readings, floor)
    html = f"<pre>{table_text}</pre>"
    await q.edit_message_text(
        html, reply_markup=main_menu_markup_for(update), parse_mode="HTML", disable_web_page_preview=True
    )
    return ConversationHandler.END


async def stats_read_choose_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "back_menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    if data not in {"stats_read_floor_1", "stats_read_floor_2"}:
        await q.edit_message_text("Пожалуйста, выберите этаж:", reply_markup=stats_floors_kb("stats_read"))
        return STATS_READ_CHOOSE_FLOOR
    floor = "1" if data.endswith("_1") else "2"
    if update.effective_user.id not in ALLOWED_USERS:
        await q.edit_message_text("⛔ У вас нет доступа к этому боту.", reply_markup=main_menu_markup_for(update))
        return ConversationHandler.END
    readings = load_json(READINGS_FP)
    table_text = build_readings_table_for_floor(readings, floor)
    html = f"<pre>{table_text}</pre>"
    await q.edit_message_text(
        html, reply_markup=main_menu_markup_for(update), parse_mode="HTML", disable_web_page_preview=True
    )
    return ConversationHandler.END


# === 10) Сборка PTB и регистрация ===
def build_ptb_app() -> Application:
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в окружении")
    app = (
        Application.builder()
        .token(TOKEN)
        .job_queue(None)
        .updater(None)
        .build()
    )

    assert app.job_queue is None, "PTB Application must not create JobQueue in webhook mode"

    # Команды
    app.add_handler(CommandHandler("start", start))

    # Админ-кнопки (вне ConversationHandler'ов)
    app.add_handler(CallbackQueryHandler(admin_db_start, pattern=r"^admin_show_tables$"))
    app.add_handler(CallbackQueryHandler(card_submission_show, pattern=r"^card_submission$"))
    app.add_handler(CallbackQueryHandler(card_submission_call, pattern=r"^call_eds$"))

    # Ввод показаний
    conv_readings = ConversationHandler(
        entry_points=[CallbackQueryHandler(open_menu_from_cb, pattern="^input_readings$")],
        states={
            CHOOSE_FLOOR: [CallbackQueryHandler(choose_floor, pattern="^(floor_1|floor_2|back_menu)$")],
            CHOOSE_MONTH: [CallbackQueryHandler(choose_month, pattern=r"^(month_\d{1,2}|back_floor|back_menu)$")],
            ENTER_DAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_day),
                CallbackQueryHandler(back_buttons, pattern="^(back_month|back_menu)$"),
            ],
            ENTER_NIGHT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_night),
                CallbackQueryHandler(back_buttons, pattern="^(back_month|back_menu)$"),
            ],
            CONFIRM_OVERWRITE: [CallbackQueryHandler(confirm_overwrite, pattern="^(ow_yes|ow_no)$")],
            CONFIRM_SAVE: [CallbackQueryHandler(confirm_save, pattern="^(save_yes|save_no)$")],
            AFTER_SAVE_PROMPT: [CallbackQueryHandler(after_save_prompt, pattern="^(add_other_yes|add_other_no)$")],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv_readings)

    # Изменение тарифов
    conv_tariffs = ConversationHandler(
        entry_points=[CallbackQueryHandler(tariffs_start, pattern="^set_tariffs$")],
        states={
            T_ENTER_GD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, tariffs_enter_gd),
                CallbackQueryHandler(back_buttons, pattern="^back_menu$"),
            ],
            T_ENTER_GN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, tariffs_enter_gn),
                CallbackQueryHandler(back_buttons, pattern="^back_menu$"),
            ],
            T_ENTER_BD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, tariffs_enter_bd),
                CallbackQueryHandler(back_buttons, pattern="^back_menu$"),
            ],
            T_ENTER_BN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, tariffs_enter_bn),
                CallbackQueryHandler(back_buttons, pattern="^back_menu$"),
            ],
            T_ENTER_EFF: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, tariffs_enter_effective_from),
                CallbackQueryHandler(back_buttons, pattern="^back_menu$"),
            ],
            T_CONFIRM_TAR: [
                CallbackQueryHandler(tariffs_confirm, pattern="^(tar_save_yes|tar_save_no)$")
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv_tariffs)

    # Расчёт «текущего»
    conv_calc_current = ConversationHandler(
        entry_points=[CallbackQueryHandler(calc_current_start, pattern="^calc_current$")],
        states={
            CALC_CHOOSE_ACTION: [
                CallbackQueryHandler(calc_saved_choose, pattern=r"^(calc_view_saved|calc_recalc|back_menu)$"),
            ],
            CALC_CONFIRM_DAYS: [
                CallbackQueryHandler(calc_days_choice, pattern=r"^(use_days_\d+|enter_days_manual|back_menu)$")
            ],
            CALC_ENTER_DAYS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, calc_enter_days),
                CallbackQueryHandler(back_buttons, pattern="^back_menu$"),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv_calc_current)

    # Показ предыдущего
    conv_show_prev = ConversationHandler(
        entry_points=[CallbackQueryHandler(show_prev_start, pattern="^show_prev$")],
        states={
            PREV_CHOOSE_MONTH: [
                CallbackQueryHandler(show_prev_choose, pattern=r"^(prev_show_\d{4}-\d{2}|back_menu|noop)$")
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv_show_prev)

    # Откат
    conv_undo = ConversationHandler(
        entry_points=[CallbackQueryHandler(undo_start, pattern="^undo_last$")],
        states={
            UNDO_CHOOSE_FLOOR: [CallbackQueryHandler(undo_choose_floor, pattern="^(floor_1|floor_2|back_menu)$")],
            UNDO_CONFIRM: [CallbackQueryHandler(undo_confirm, pattern=r"^(undo_yes_\d{4}-\d{2}_(1|2)|undo_no)$")],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv_undo)

    # Статистика
    conv_stats = ConversationHandler(
        entry_points=[CallbackQueryHandler(stats_start, pattern="^stats_menu$")],
        states={
            STATS_CHOOSE_TYPE: [
                CallbackQueryHandler(stats_choose_type, pattern="^(stats_cons|stats_read|back_menu)$"),
            ],
            STATS_CONS_CHOOSE_FLOOR: [
                CallbackQueryHandler(
                    stats_cons_choose_floor,
                    pattern="^(stats_cons_floor_1|stats_cons_floor_2|back_menu)$",
                )
            ],
            STATS_READ_CHOOSE_FLOOR: [
                CallbackQueryHandler(
                    stats_read_choose_floor,
                    pattern="^(stats_read_floor_1|stats_read_floor_2|back_menu)$",
                )
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv_stats)

    # Глобальный обработчик «В меню» — ПОСЛЕ всех ConversationHandler
    async def back_to_menu_global(update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_markup_for(update))

    app.add_handler(CallbackQueryHandler(back_to_menu_global, pattern=r"^back_menu$"))

    return app


# === 11) FASTAPI + lifecycle PTB ===
ptb_app = build_ptb_app()

async def _ensure_webhook_current():
    desired = _desired_webhook_url()
    if not desired:
        return
    info = await ptb_app.bot.get_webhook_info()
    if info.url != desired:
        await ptb_app.bot.set_webhook(
            url=desired,
            allowed_updates=[],
            drop_pending_updates=False,
            secret_token=os.environ.get("WEBHOOK_SECRET") or None,
        )

async def webhook_watchdog(bot: Bot):
    try:
        desired = _desired_webhook_url()
        info = await bot.get_webhook_info()
        mismatch = bool(desired) and (info.url != desired)
        has_error = bool(getattr(info, "last_error_message", None))
        if mismatch or has_error:
            logging.warning("Webhook watchdog: mismatch=%s error=%s",
                            mismatch, getattr(info, "last_error_message", ""))
            await bot.set_webhook(
                url=desired or info.url,
                allowed_updates=[],
                drop_pending_updates=False,
                secret_token=os.environ.get("WEBHOOK_SECRET") or None,
            )
    except TelegramError:
        logging.exception("Webhook watchdog failed")


async def _webhook_watchdog_loop(bot: Bot, interval_sec: int) -> None:
    while True:
        await webhook_watchdog(bot)
        await asyncio.sleep(interval_sec)


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with ptb_app:
        await ptb_app.start()
        watchdog_task = asyncio.create_task(_webhook_watchdog_loop(ptb_app.bot, 900))
        try:
            await _ensure_webhook_current()   # проверяем/чиним вебхук на старте
            yield                              # тут работает FastAPI
        finally:
            # аккуратно гасим фоновые задачи
            watchdog_task.cancel()
            with suppress(asyncio.CancelledError):
                await watchdog_task
            # и только затем останавливаем PTB
            await ptb_app.stop()


app = FastAPI(lifespan=lifespan)

# GET 
@app.get("/healthz", include_in_schema=False)
async def healthz():
    return {"ok": True}

# ДОБАВЬ отдельный HEAD-обработчик
@app.head("/healthz", include_in_schema=False)
async def healthz_head():
    # для HEAD тело не нужно — просто 200
    return Response(status_code=200)


@app.post("/webhook")
async def telegram_webhook(request: Request):
    # Проверка секрета, если включён
    secret = os.environ.get("WEBHOOK_SECRET")
    if secret:
        hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if not hdr or not hmac.compare_digest(hdr, secret):
            return Response(status_code=HTTPStatus.UNAUTHORIZED)
    # Обработка апдейта
    update = Update.de_json(await request.json(), ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=HTTPStatus.OK)


def _desired_webhook_url() -> Optional[str]:
    public_url = os.environ.get("PUBLIC_URL")
    if not public_url:
        return None
    return public_url.rstrip("/") + "/webhook"

@app.get("/set_webhook")
async def set_webhook():
    url = _desired_webhook_url()
    if not url:
        return {"ok": False, "error": "PUBLIC_URL не задан в окружении"}
    ok = await ptb_app.bot.set_webhook(
        url=url,
        allowed_updates=[],              # явный сброс фильтров
        drop_pending_updates=False,
        secret_token=os.environ.get("WEBHOOK_SECRET") or None,
    )
    return {"ok": ok, "url": url}

@app.get("/reset_webhook")
async def reset_webhook():
    ok = await ptb_app.bot.delete_webhook(drop_pending_updates=True)
    return {"ok": ok}