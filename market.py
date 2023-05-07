import csv
import time
from pprint import pprint  # pylint: disable=unused-import

import gspread
import google.auth.transport.requests as greq
from polygon import RESTClient, exceptions

import date_helpers as dh
from config import config

conf = config.conf

# Google sheets override default timeout 120s -> 600s
greq._DEFAULT_TIMEOUT = 600  # pylint: disable=protected-access


class MarketData:
    """
    MarketData
    """

    rate_limit = 12.1
    multiplier = 1

    # csvs
    agg_csv = conf.csv_files.aggregate
    monthlies = conf.csv_files.monthlies
    weeklies = conf.csv_files.weeklies

    def __init__(self, samples=13, min_samples=9, timespan="week", from_=None, to=None):
        self.samples = samples
        self.min_samples = min_samples
        self.timespan = timespan
        self.to = to or dh.most_recent_saturday().isoformat()
        self.from_ = from_ or dh.x_saturdays_ago(samples).isoformat()

        self.tickers_csv = self.weeklies
        if (
            dh.current_monthly_expr() == dh.current_expr()
            or dh.current_monthly_expr() == dh.next_expr()
        ):
            self.tickers_csv = self.monthlies

        self.all_lines = []

    def exec(self):
        print(f"Fetching stock data from {self.from_} to {self.to} ...")
        for ticker in self.get_tickers(self.tickers_csv):
            res = self.weekly_stock_data(ticker)
            if not res:
                print(f"{ticker} - Error: Polygon returned no results")
                continue

            if len(res) < self.min_samples:
                print(
                    f"{ticker} - Info: requires at least {self.min_samples} \
                        weeks of data. Only has {len(res)}. Skipping..."
                )
                continue

            print(ticker)

            lines = []
            for d in res:
                # ticker, timestamp, o, h, l, c, vw, v, n
                line = [
                    ticker,
                    d.timestamp,
                    d.open,
                    d.high,
                    d.low,
                    d.close,
                    d.vwap,
                    d.volume,
                    d.transactions,
                ]
                lines.append(line)
                self.all_lines.append(line)

            self.write_to_csv(lines)
            time.sleep(self.rate_limit)

    def get_tickers(self, filename):
        return list(map(lambda x: x[0], self.read_csv(filename, delimiter=",")))

    # https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksTicker__range__multiplier___timespan___from___to
    def weekly_stock_data(self, ticker, multiplier=multiplier):
        client = RESTClient(conf.polygon.api_key)
        while True:
            try:
                res = client.get_aggs(
                    ticker, multiplier, self.timespan, self.from_, self.to
                )
                break
            except exceptions.NoResultsError:
                print(f"No resultts for ${ticker} ... Skipping")
                return None
            except Exception:
                print("Request failed. Retrying ...")
                time.sleep(self.rate_limit)

        return res

    @staticmethod
    def write_to_csv(lines, filename=agg_csv):
        with open(filename, "a", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerows(lines)

    @staticmethod
    def read_csv(filename, delimiter="\t"):
        with open(filename, "r", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=delimiter)
            return list(map(lambda x: x, csv_reader))

    def upload_to_google_sheets(self):
        print("Uploading to google sheets ...")

        gc = gspread.service_account()
        sheet = gc.open("stonks analysis")
        worksheet = sheet.worksheet("agg")

        if not self.all_lines:
            self.all_lines = self.read_csv(self.agg_csv)

        rows = len(self.all_lines)
        cols = len(self.all_lines[0])

        worksheet.resize(rows + 2, cols)

        batch = []
        for i, row in enumerate(self.all_lines, start=3):
            batch.append({"range": f"A{i}:I{i}", "values": [row]})

        worksheet.batch_update(batch, value_input_option="USER_ENTERED")


if __name__ == "__main__":
    md = MarketData()
    md.exec()
    md.upload_to_google_sheets()
