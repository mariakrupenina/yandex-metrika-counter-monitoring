"""
Мониторинг поступления данных в Яндекс Метрику через Reports API

Скрипт предназначен для автоматической проверки, что данные по счетчикам
продолжают поступать корректно. Проверка выполняется по двум метрикам:
pageviews и visits.

Подход:
- каждые 30 минут анализируется предыдущий завершенный 30-минутный слот;
- для каждого счетчика рассчитываются текущие значения метрик;
- в качестве baseline используется медиана значений за тот же слот
  за предыдущие 7 дней;
- если обе метрики одновременно обнуляются или падают ниже 20% от baseline,
  счетчик считается отвалившимся.

Дополнительно:
- поддерживается индивидуальный мониторинг по каждому счетчику;
- поддерживается групповой мониторинг для основной группы сайтов;
- на основе результата могут формироваться email-уведомления об инциденте

Скрипт может использоваться как основа для Airflow DAG
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

import smtplib
from email.mime.text import MIMEText

load_dotenv()

#OAuth-токен Яндекс Метрики для авторизации в API
def get_token():
    token = os.getenv("YM_OAUTH_TOKEN")
    if not token:
        raise ValueError("YM_OAUTH_TOKEN is not set")
    return token

#счётчики
def get_counters():
    counters = {
        "SBL": os.getenv("YM_COUNTER_SBL"),
        "PRO": os.getenv("YM_COUNTER_PRO"),
        "CIB": os.getenv("YM_COUNTER_CIB"),
        "INDIA": os.getenv("YM_COUNTER_INDIA"),
        "LEGAL": os.getenv("YM_COUNTER_LEGAL"),
    }

    empty_keys = [key for key, value in counters.items() if not value]
    if empty_keys:
        raise ValueError(f"Counter IDs are not set for: {', '.join(empty_keys)}")

    return {key: int(value) for key, value in counters.items()}

GROUP_COUNTERS = ["SBL", "PRO", "CIB", "INDIA"]

THRESHOLD_RATIO = 0.2 #порог просадки: если текущее значение меньше 20% от baseline, метрика считается аномально низкой
LOOKBACK_DAYS = 7 #медиана за 7 дней



#функция, которая определяет какой последний 30-минутный слот нужно проверять
#если скрипт запустился в 10:00, проверяем слот 09:30–09:59
#если в 10:30, проверяем слот 10:00–10:29
def get_last_completed_slot(now: datetime | None = None):
    if now is None:
        now = datetime.now()

    minute = now.minute

    if minute < 30:
        slot_end = now.replace(minute=0, second=0, microsecond=0) - timedelta(minutes=1)
    else:
        slot_end = now.replace(minute=30, second=0, microsecond=0) - timedelta(minutes=1)

    slot_start = slot_end - timedelta(minutes=29)

    return slot_start, slot_end



#функция формирует список исторических 30-минутных слотов для расчета baseline
# Параметры:
# - slot_start: начало текущего проверяемого слота
# - lookback_days: сколько предыдущих дней берем в историю
def get_historical_slots(slot_start: datetime, lookback_days: int = LOOKBACK_DAYS):
    slots = []

    for days_ago in range(1, lookback_days + 1):
        hist_start = slot_start - timedelta(days=days_ago)
        hist_end = hist_start + timedelta(minutes=29)
        slots.append((hist_start, hist_end))

    return slots



# Функция запрашивает из Reports API Яндекс Метрики
# значения pageviews и visits за конкретный временной интервал
# Возвращает словарь:
# {
#     "pageviews": ...,
#     "visits": ...
# }
def fetch_interval_metrics(counter_id: int, start_dt: datetime, end_dt: datetime):
    #Endpoint Reports API для получения агрегированных метрик
    url = "https://api-metrika.yandex.net/stat/v1/data"

    headers = {
        "Authorization": f"OAuth {get_token()}"
    }

    params = {
        "ids": counter_id,
        "metrics": "ym:s:pageviews,ym:s:visits",
        "date1": start_dt.strftime("%Y-%m-%d"),
        "date2": end_dt.strftime("%Y-%m-%d"),
        "filters": (
            f"ym:s:dateTime>='{start_dt.strftime('%Y-%m-%d %H:%M:%S')}' AND "
            f"ym:s:dateTime<='{end_dt.strftime('%Y-%m-%d %H:%M:%S')}'"
        ),
        "accuracy": "full"
    }

    #отправляем запрос в API Метрики
    response = requests.get(url, headers=headers, params=params, timeout=60)
    # response.raise_for_status()

    #если API вернул ошибку, печатаем детали для отладки
    if response.status_code != 200:
        print("ERROR STATUS:", response.status_code)
        print("ERROR TEXT:", response.text)
        print("COUNTER ID:", counter_id)
        print("START:", start_dt)
        print("END:", end_dt)
        print("PARAMS:", params)
        raise Exception("Metrika API request failed")

    data = response.json()

    if not data.get("data"):
        return {
            "pageviews": 0.0,
            "visits": 0.0
        }

    metrics = data["data"][0]["metrics"]

    return {
        "pageviews": float(metrics[0] or 0),
        "visits": float(metrics[1] or 0)
    }



# Функция собирает исторические значения метрик по счетчику
# для списка одинаковых 30-минутных слотов за прошлые дни.
#
# На вход:
# - counter_id: ID счетчика Метрики
# - historical_slots: список кортежей (начало, конец) для исторических интервалов
#
# На выход:
# - DataFrame, где каждая строка соответствует одному историческому слоту
#   и содержит pageviews и visits за этот интервал
def fetch_historical_metrics(counter_id: int, historical_slots: list[tuple[datetime, datetime]]):
    rows = []

    for hist_start, hist_end in historical_slots:
        metrics = fetch_interval_metrics(
            counter_id=counter_id,
            start_dt=hist_start,
            end_dt=hist_end
        )

        rows.append({
            "slot_start": hist_start,
            "slot_end": hist_end,
            "pageviews": metrics["pageviews"],
            "visits": metrics["visits"]
        })

    return pd.DataFrame(rows)



# Функция рассчитывает baseline по историческим значениям метрик
# В качестве baseline используем медиану за 7 предыдущих дней
def calculate_baseline(hist_df: pd.DataFrame):
    return {
        "pageviews_median": float(hist_df["pageviews"].median()),
        "visits_median": float(hist_df["visits"].median())
    }



# Функция сравнивает текущие значения метрик с baseline
# и рассчитывает ratio для pageviews и visits.
#
# ratio = current / baseline
#
# Интерпретация ratio:
# - 1.0  -> текущее значение равно норме
# - 0.0  -> данных нет
#
# Возвращаем не только ratio, но и сами значения,
# чтобы потом использовать их в логах и email-алертах
def compare_with_baseline(current_metrics: dict, baseline: dict):
    pageviews_current = float(current_metrics["pageviews"])
    visits_current = float(current_metrics["visits"])

    pageviews_baseline = float(baseline["pageviews_median"])
    visits_baseline = float(baseline["visits_median"])

    pageviews_ratio = (
        pageviews_current / pageviews_baseline
        if pageviews_baseline > 0 else 0.0
    )
    visits_ratio = (
        visits_current / visits_baseline
        if visits_baseline > 0 else 0.0
    )

    return {
        "pageviews_current": pageviews_current,
        "visits_current": visits_current,
        "pageviews_baseline": pageviews_baseline,
        "visits_baseline": visits_baseline,
        "pageviews_ratio": pageviews_ratio,
        "visits_ratio": visits_ratio,
    }



# Функция применяет правило мониторинга и определяет,
# считается ли счетчик отвалившимся
#
# Логика:
# - pageviews считаются просевшими, если они равны нулю
#   или меньше THRESHOLD_RATIO от baseline
# - visits считаются просевшими по тому же правилу
# - счетчик считается отвалившимся только если одновременно
#   просели обе метрики
def detect_counter_down(comparison: dict):
    is_down_by_pageviews = (
        comparison["pageviews_current"] == 0
        or comparison["pageviews_ratio"] < THRESHOLD_RATIO
    )

    is_down_by_visits = (
        comparison["visits_current"] == 0
        or comparison["visits_ratio"] < THRESHOLD_RATIO
    )
    # Итоговый флаг отвала:
    # счетчик считаем отвалившимся только если обе метрики одновременно аномальны
    is_counter_down = is_down_by_pageviews and is_down_by_visits

    return {
        "is_down_by_pageviews": is_down_by_pageviews,
        "is_down_by_visits": is_down_by_visits,
        "is_counter_down": is_counter_down,
    }



# Основная функция проверки одного счетчика
#
# Что делает:
# 1. Получает текущие значения pageviews и visits за проверяемый слот
# 2. Формирует список исторических слотов за прошлые 7 дней
# 3. Получает исторические значения метрик
# 4. Считает baseline как медиану
# 5. Сравнивает текущие значения с baseline
# 6. Определяет, считается ли счетчик отвалившимся
#
# Возвращает словарь со всеми результатами проверки по счетчику
def check_one_counter(counter_name: str, counter_id: int, slot_start: datetime, slot_end: datetime):
    current_metrics = fetch_interval_metrics(
        counter_id=counter_id,
        start_dt=slot_start,
        end_dt=slot_end
    )

    historical_slots = get_historical_slots(slot_start, LOOKBACK_DAYS)

    hist_df = fetch_historical_metrics(
        counter_id=counter_id,
        historical_slots=historical_slots
    )

    baseline = calculate_baseline(hist_df)

    comparison = compare_with_baseline(
        current_metrics=current_metrics,
        baseline=baseline
    )

    status = detect_counter_down(comparison)

    return {
        "counter_name": counter_name,
        "counter_id": counter_id,
        "slot_start": slot_start,
        "slot_end": slot_end,
        **comparison,
        **status
    }



# Функция запускает проверку по всем счетчикам из словаря COUNTERS
#
# Для каждого счетчика вызывает check_one_counter(),
# собирает результаты в список и преобразует его в DataFrame
#
# На выходе получаем общую таблицу проверки по всем счетчикам
def check_all_counters(slot_start: datetime, slot_end: datetime):
    results = []

    for counter_name, counter_id in get_counters().items():
        result = check_one_counter(
            counter_name=counter_name,
            counter_id=counter_id,
            slot_start=slot_start,
            slot_end=slot_end
        )
        results.append(result)

    return pd.DataFrame(results)



# Функция определяет, есть ли групповой отвал
#
# В групповой алерт входят только счетчики из GROUP_COUNTERS
# Групповой отвал считаем произошедшим, если одновременно
# отвалились минимум 2 счетчика из этой группы
def detect_group_down(results_df: pd.DataFrame):
    group_df = results_df[results_df["counter_name"].isin(GROUP_COUNTERS)].copy()

    down_counters = group_df[group_df["is_counter_down"] == True]["counter_name"].tolist()
    group_is_down = len(down_counters) >= 2

    return {
        "group_is_down": group_is_down,
        "down_counters": down_counters,
        "down_counters_count": len(down_counters)
    }



# Вспомогательная функция:
# возвращает только те счетчики, которые признаны отвалившимися
def get_down_counters(results_df: pd.DataFrame):
    down_df = results_df[results_df["is_counter_down"] == True].copy()
    return down_df


# Функция формирует текст индивидуального email-алерта
# для случая, когда отвалился ОДИН счетчик
#
# На вход получает одну строку из results_df
# На выходе возвращает тему и текст письма
def build_individual_alert_message(row: pd.Series):
    subject = f"[ALERT] Отвал счетчика {row['counter_name']}"

    body = (
        f"Обнаружен отвал счетчика: {row['counter_name']} (ID: {row['counter_id']})\n\n"
        f"Интервал проверки: {row['slot_start']} — {row['slot_end']}\n\n"
        f"Просмотры: сейчас — {row['pageviews_current']}, медиана — {row['pageviews_baseline']}\n"
        f"Визиты: сейчас — {row['visits_current']}, медиана — {row['visits_baseline']}\n"
    )

    return subject, body



# Функция формирует текст общего email-алерта
# для случая, когда одновременно отвалились НЕСКОЛЬКО счетчиков
#
# На вход получает DataFrame только с отвалившимися счетчиками
# На выходе возвращает тему и текст письма
def build_group_alert_message(down_df: pd.DataFrame):
    # Собираем названия счетчиков в одну строку для темы письма
    counter_names = ", ".join(down_df["counter_name"].tolist())

    # Берем первую строку, чтобы показать интервал проверки:
    # он одинаковый для всех счетчиков в одном запуске
    first_row = down_df.iloc[0]

    subject = f"[ALERT] Групповой отвал счетчиков: {counter_names}"

    lines = [
        f"Обнаружен групповой отвал счетчиков: {counter_names}",
        "",
        f"Интервал проверки: {first_row['slot_start']} — {first_row['slot_end']}",
        ""
    ]

    # Добавляем в письмо краткую информацию по каждому счетчику
    for _, row in down_df.iterrows():
        lines.extend([
            f"{row['counter_name']} (ID: {row['counter_id']})",
            f"Просмотры: сейчас — {row['pageviews_current']}, медиана — {row['pageviews_baseline']}",
            f"Визиты: сейчас — {row['visits_current']}, медиана — {row['visits_baseline']}",
            ""
        ])

    body = "\n".join(lines)
    return subject, body


# Функция решает, нужно ли отправлять алерт,
# и если нужно — формирует ровно одно письмо.
#
# Логика:
# - если отвалившихся счетчиков нет, возвращаем None
# - если отвалился один счетчик, формируем индивидуальное письмо
# - если отвалилось два и более счетчика, формируем одно общее письмо
#
# Таким образом, в одном запуске скрипта отправляется максимум одно письмо
def collect_alerts(results_df: pd.DataFrame):
    # Оставляем только отвалившиеся счетчики
    down_df = get_down_counters(results_df)

    # Если отвалов нет, письмо не нужно
    if down_df.empty:
        return None

    # Если отвалился только один счетчик, формируем индивидуальный алерт
    if len(down_df) == 1:
        row = down_df.iloc[0]
        subject, body = build_individual_alert_message(row)
        return {
            "type": "individual",
            "subject": subject,
            "body": body
        }

    # Если отвалилось несколько счетчиков, формируем одно общее письмо
    subject, body = build_group_alert_message(down_df)
    return {
        "type": "group",
        "subject": subject,
        "body": body
    }


# Функция отправляет email-уведомление через SMTP Яндекса.
#
# На вход получает:
# - subject: тема письма
# - body: текст письма
#
# Логин, пароль и список получателей читаются из .env.

def send_email(subject: str, body: str):
    # Читаем SMTP-логин и пароль из переменных окружения
    login = os.getenv("SMTP_LOGIN")
    password = os.getenv("SMTP_PASSWORD")

    # Получателей можно передавать списком через запятую в EMAIL_TO
    recipients = [email.strip() for email in os.getenv("EMAIL_TO", "").split(",") if email.strip()]

    # Если SMTP_LOGIN не задан, завершаем выполнение с ошибкой
    if not login:
        raise ValueError("SMTP_LOGIN is not set")

    # Если SMTP_PASSWORD не задан, завершаем выполнение с ошибкой
    if not password:
        raise ValueError("SMTP_PASSWORD is not set")

    # Если список получателей пустой, завершаем выполнение с ошибкой
    if not recipients:
        raise ValueError("EMAIL_TO is empty")

    # Формируем текстовое письмо
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = login
    msg["To"] = ", ".join(recipients)

    # Подключаемся к SMTP-серверу Яндекса, включаем TLS и отправляем письмо
    with smtplib.SMTP("smtp.yandex.ru", 587, timeout=60) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(login, password)
        server.send_message(msg)


def main():
    slot_start, slot_end = get_last_completed_slot()

    print(f"[INFO] Проверяем слот: {slot_start} — {slot_end}")

    results_df = check_all_counters(
        slot_start=slot_start,
        slot_end=slot_end
    )

    print("[INFO] Результаты проверки:")
    print(results_df.to_string(index=False))

    alert = collect_alerts(results_df)

    if alert is None:
        print("[INFO] Отвалов не найдено, письмо не отправляем")
        return

    print(f"[INFO] Найден алерт типа: {alert['type']}")
    print(f"[INFO] Тема письма: {alert['subject']}")

    send_email(
        subject=alert["subject"],
        body=alert["body"]
    )

    print("[INFO] Письмо отправлено")


if __name__ == "__main__":
    main()
