import pandas
import pandas_datareader.data as web
import requests

###
# MAX 250 requests/day
financial_modelling_prep_ApiKey = "45096d60957d845579f02d3b3c389aa0"

fx_pairs = ['EUR/USD', 'USD/JPY', 'GBP/USD', 'EUR/GBP', 'USD/CHF', 'EUR/JPY', 'EUR/CHF', 'USD/CAD', 'AUD/USD',
            'GBP/JPY', 'AUD/CAD', 'AUD/CHF', 'AUD/JPY', 'AUD/NZD', 'CAD/CHF', 'CAD/JPY', 'CHF/JPY', 'EUR/AUD',
            'EUR/CAD', 'EUR/NOK', 'EUR/NZD', 'GBP/CAD', 'GBP/CHF', 'NZD/JPY', 'NZD/USD', 'USD/NOK', 'USD/SEK',
            'EU50/EUR', 'USD/INR', 'AUD/HKD', 'US30/USD', 'UK10YB/GBP', 'NZD/CHF', 'HK33/HKD', 'GBP/SGD', 'XAU/AUD',
            'SG30/SGD', 'XAU/NZD', 'SGD/JPY', 'NZD/CAD', 'DE10YB/EUR', 'SGD/HKD', 'SOYBN/USD', 'XAG/AUD', 'USB30Y/USD',
            'XPT/USD', 'XAU/JPY', 'XAU/GBP', 'CORN/USD', 'EUR/TRY', 'XAU/EUR', 'XAG/CHF', 'NL25/EUR', 'NZD/SGD',
            'GBP/PLN', 'EUR/SGD', 'WTICO/USD', 'XAU/SGD', 'AUD/SGD', 'TRY/JPY', 'XAU/USD', 'EUR/CZK', 'XAU/HKD',
            'TWIX/USD', 'NAS100/USD', 'USD/ZAR', 'CHF/HKD', 'CAD/HKD', 'EUR/HUF', 'AU200/AUD', 'XAG/USD', 'USD/SAR',
            'USD/TRY', 'EUR/DKK', 'EUR/PLN', 'EUR/HKD', 'FR40/EUR', 'CN50/USD', 'XAG/EUR', 'XAU/CHF', 'USD/THB',
            'USB05Y/USD', 'USD/HUF', 'NATGAS/USD', 'XPD/USD', 'UK100/GBP', 'GBP/AUD', 'XAG/CAD', 'SGD/CHF', 'IN50/USD',
            'GBP/NZD', 'BCO/USD', 'JP225/USD', 'XAG/SGD', 'ZAR/JPY', 'WHEAT/USD', 'DE30/EUR', 'USD/PLN', 'USD/MXN',
            'XAG/HKD', 'HKD/JPY', 'CHF/ZAR', 'CAD/SGD', 'USD/CZK', 'XCU/USD', 'USD/DKK', 'US2000/USD', 'GBP/HKD',
            'SUGAR/USD', 'USB10Y/USD', 'XAG/JPY', 'XAG/GBP', 'EUR/SEK', 'USD/SGD', 'SPX500/USD', 'XAU/XAG', 'USD/HKD',
            'EUR/ZAR', 'USB02Y/USD', 'XAG/NZD', 'XAU/CAD', 'NZD/HKD', 'USD/CNH', 'GBP/ZAR']

# url = f"https://financialmodelingprep.com/api/v3/fx?apikey={financial_modelling_prep_ApiKey}"
# response = requests.get(url).json()
# example_quote = [{'ticker': 'EUR/USD', 'bid': '1.09376', 'ask': '1.09376', 'open': '1.10640', 'low': '1.08858', 'high': '1.10688', 'changes': -0.01142443962400577, 'date': '2022-03-06 03:12:06'}]


from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()

# response = cg.get_price(ids='tether', vs_currencies='pln')
# example_response = {'tether': {'pln': 4.48}}

