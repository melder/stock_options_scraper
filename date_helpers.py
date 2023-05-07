from config import config  # pylint: disable=wrong-import-order

from datetime import date, datetime, timedelta
from pprint import pprint as pp  # pylint: disable=unused-import


UNEXPIRED_DATES_KEY = "expr_dates:unexpired"
ALL_EXPIRATION_DATES_KEY = "expr_dates:all"


def all_exprs():
    return list(sorted(config.r.smembers(ALL_EXPIRATION_DATES_KEY)))


def unexpired():
    return list(sorted(config.r.smembers(UNEXPIRED_DATES_KEY)))


def current_expr():
    return unexpired()[0]


def next_expr():
    return unexpired()[1]


# month == "01" to "12"
def third_expr_of_month(year, month):
    res = []
    for e in all_exprs():
        _year, _month, _ = e.split("-")
        if month == _month and year == _year:
            res.append(e)
    res.sort()
    return res[2]


def current_monthly_expr():
    today = datetime.utcnow()
    year = str(today.year)
    month = str(today.month).zfill(2)
    return third_expr_of_month(year, month)


def most_recent_saturday(d=date.today()):
    weekday = d.weekday()
    weeks = 0 if weekday < 5 else 1
    return d + timedelta(days=-weekday - 2, weeks=weeks)


def x_saturdays_ago(x, d=date.today()):
    return most_recent_saturday(d) + timedelta(weeks=-(x - 1))
