import codecs
import csv
import pprint  # pylint: disable=unused-import
import time
import urllib.request

from polygon import RESTClient

from config import config
conf = config.conf

POLYGON_SLEEP_TIME = 15.1  # rate limited to 4 requests per minute
STOCK_TYPES = ["CS", "ADRC"]


def write_to_csv(filename, lines, delimiter="\t"):
    with open(filename, "w", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter)
        writer.writerows(lines)


def get_tickers_polygon():
    tickers = []
    client = RESTClient(conf.polygon.api_key)
    res = client.list_tickers(limit=1000)
    for idx, t in enumerate(res):
        if idx % 1000 == 0 and idx > 0:
            time.sleep(POLYGON_SLEEP_TIME)
        if t.type and t.type in STOCK_TYPES:
            tickers.append(t.ticker)
    return tickers


def get_all_options_cboe():
    url = "https://www.cboe.com/us/options/symboldir/?download=csv"
    csv_data = urllib.request.urlopen(url)
    csvfile = csv.reader(codecs.iterdecode(csv_data, "utf-8"))
    return list(csvfile)


def get_all_weekly_options_cboe():
    url = "https://www.cboe.com/us/options/symboldir/weeklys_options/?download=csv"
    csv_data = urllib.request.urlopen(url)
    csvfile = csv.reader(codecs.iterdecode(csv_data, "utf-8"))
    return list(csvfile)


def get_all_option_tickers():
    res = set()
    for line in get_all_options_cboe():
        res.add(line[1])
    return list(res)


def get_weekly_option_tickers():
    res = set()
    for line in get_all_weekly_options_cboe():
        res.add(line[1])
    return list(res)


def intersect(a, b):
    res = list(set(a) & set(b))
    res.sort()
    return res


if __name__ == "__main__":
    tickers_poly = get_tickers_polygon()
    # need to embed every symbol into array for csv to write properly
    write_to_csv(conf.csv_files.symbols, map(lambda x: [x], tickers_poly))

    tickers_cboe = get_all_option_tickers()
    tickers_cboe.sort()
    write_to_csv(conf.csv_files.option_monthlies, map(lambda x: [x], tickers_cboe[1:]))

    tickers_weeklies_cboe = get_weekly_option_tickers()
    tickers_weeklies_cboe.sort()
    write_to_csv(
        conf.csv_files.option_weeklies, map(lambda x: [x], tickers_weeklies_cboe[1:])
    )

    monthlies = intersect(tickers_poly, tickers_cboe)
    write_to_csv(conf.csv_files.monthlies, map(lambda x: [x], monthlies))

    weeklies = intersect(tickers_poly, tickers_weeklies_cboe)
    write_to_csv(conf.csv_files.weeklies, map(lambda x: [x], weeklies))
