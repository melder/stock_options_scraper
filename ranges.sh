rm csv/aggregate.csv

pipenv run python tickers.py
pipenv run python market.py

git add csv
git commit -m "$(date --iso-8601) -- updates csvs"
git push origin master