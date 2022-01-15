import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import logging
import matplotlib.pyplot as plt

logging.basicConfig(format="%(asctime)s.%(msecs)03d %(message)s",
                    datefmt='%H:%M:%S'
                    )

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dev_ex_dict = {
    'zonda': "https://www.coingecko.com/en/exchanges/bitbay",
    "gemini": "https://www.coingecko.com/en/exchanges/gemini",
    "binanceus": "https://www.coingecko.com/en/exchanges/binance_us",
    "bitkub": "https://www.coingecko.com/en/exchanges/bitkub",
    "bitso": "https://www.coingecko.com/en/exchanges/bitso",
    "cex": "https://www.coingecko.com/en/exchanges/cex",
    "bitfinex": "https://www.coingecko.com/en/exchanges/bitfinex",
    "okex": "https://www.coingecko.com/en/exchanges/okex",
    "crypto.com": "https://www.coingecko.com/en/exchanges/crypto_com",
    "huobi": "https://www.coingecko.com/en/exchanges/huobi",
    "kraken": "https://www.coingecko.com/en/exchanges/kraken",
    "bybit": "https://www.coingecko.com/en/exchanges/bybit_spot"
}


class StaticSymbolAnalyzer:
    def __init__(self, arg: str):
        self.arg = arg
        self.ex_frames_dict = {}
        self.symbol_dataset = {}

    @staticmethod
    def main_table_scrapper(url):
        response = requests.get(url)
        soup = bs(response.content, features="lxml")
        table = soup.find_all('table', attrs={'class': 'table table-scrollable'})
        df = pd.read_html(str(table))[0]
        df['Coin 24h Volume'] = df['24h Volume'].apply(lambda x: x.split(" ")[2])
        df['Price_USD'] = df['Price'].apply(lambda x: float(x.split("$")[1].split(" ")[0].replace(",", "")))
        df.set_index(df['#'], inplace=True)
        logger.info(f"Getting market stats for {url.split('/')[5]}")
        return df[['Pair', 'Price_USD', 'Spread', 'Coin 24h Volume']]

    def prepare_data(self):

        for k, v in dev_ex_dict.items():
            self.ex_frames_dict[k] = self.main_table_scrapper(v)

        for k, v in self.ex_frames_dict.items():
            for element in v['Pair']:
                if element.split("/")[0] == self.arg:
                    self.symbol_dataset[k] = v.loc[v['Pair'] == element].values()
                    print(self.symbol_dataset[k])

    def perform_static_analysis(self):

        for k, v in self.symbol_dataset.items():
            print(
                f"{k} : \nPrice in USD : {v['Price_USD']} \n24 Volume : {v['Coin 24h Volume']} \nSpread : {v['Spread']}")
            plt.scatter(k, v['Coin 24h Volume'])

        plt.show()


if __name__ == '__main__':
    symb_cons = StaticSymbolAnalyzer('ADA')
    symb_cons.prepare_data()
    # symb_cons.perform_static_analysis()
